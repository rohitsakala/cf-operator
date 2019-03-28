package manifest_test

import (
	"code.cloudfoundry.org/cf-operator/pkg/bosh/manifest"
	"code.cloudfoundry.org/cf-operator/testing"

	esv1 "code.cloudfoundry.org/cf-operator/pkg/kube/apis/extendedsecret/v1alpha1"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
)

var _ = Describe("ConvertToKube", func() {
	var (
		m          manifest.Manifest
		kubeConfig manifest.KubeConfig
		env        testing.Catalog
	)

	BeforeEach(func() {
		m = env.DefaultBOSHManifest()
		format.TruncatedDiff = false
	})

	Context("converting variables", func() {
		It("sanitizes secret names", func() {
			m.Name = "-abc_123.?!\"§$&/()=?"
			m.Variables[0].Name = "def-456.?!\"§$&/()=?-"

			kubeConfig, _ = m.ConvertToKube("foo")
			Expect(kubeConfig.Variables[0].Name).To(Equal("abc-123.var-def-456"))
		})

		It("trims secret names to 63 characters", func() {
			m.Name = "foo"
			m.Variables[0].Name = "this-is-waaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaay-too-long"

			kubeConfig, _ = m.ConvertToKube("foo")
			Expect(kubeConfig.Variables[0].Name).To(Equal("foo.var-this-is-waaaaaaaaaaaaaa5bffdb0302ac051d11f52d2606254a5f"))
		})

		It("converts password variables", func() {
			kubeConfig, _ = m.ConvertToKube("foo")
			Expect(len(kubeConfig.Variables)).To(Equal(1))

			var1 := kubeConfig.Variables[0]
			Expect(var1.Name).To(Equal("foo-deployment.var-adminpass"))
			Expect(var1.Spec.Type).To(Equal(esv1.Password))
			Expect(var1.Spec.SecretName).To(Equal("foo-deployment.var-adminpass"))
		})

		It("converts rsa key variables", func() {
			m.Variables[0] = manifest.Variable{
				Name: "adminkey",
				Type: "rsa",
			}
			kubeConfig, _ = m.ConvertToKube("foo")
			Expect(len(kubeConfig.Variables)).To(Equal(1))

			var1 := kubeConfig.Variables[0]
			Expect(var1.Name).To(Equal("foo-deployment.var-adminkey"))
			Expect(var1.Spec.Type).To(Equal(esv1.RSAKey))
			Expect(var1.Spec.SecretName).To(Equal("foo-deployment.var-adminkey"))
		})

		It("converts ssh key variables", func() {
			m.Variables[0] = manifest.Variable{
				Name: "adminkey",
				Type: "ssh",
			}
			kubeConfig, _ = m.ConvertToKube("foo")
			Expect(len(kubeConfig.Variables)).To(Equal(1))

			var1 := kubeConfig.Variables[0]
			Expect(var1.Name).To(Equal("foo-deployment.var-adminkey"))
			Expect(var1.Spec.Type).To(Equal(esv1.SSHKey))
			Expect(var1.Spec.SecretName).To(Equal("foo-deployment.var-adminkey"))
		})

		It("converts certificate variables", func() {
			m.Variables[0] = manifest.Variable{
				Name: "foo-cert",
				Type: "certificate",
				Options: &manifest.VariableOptions{
					CommonName:       "example.com",
					AlternativeNames: []string{"foo.com", "bar.com"},
					IsCA:             true,
					CA:               "theca",
					ExtendedKeyUsage: []manifest.AuthType{manifest.ClientAuth},
				},
			}
			kubeConfig, _ = m.ConvertToKube("foo")
			Expect(len(kubeConfig.Variables)).To(Equal(1))

			var1 := kubeConfig.Variables[0]
			Expect(var1.Name).To(Equal("foo-deployment.var-foo-cert"))
			Expect(var1.Spec.Type).To(Equal(esv1.Certificate))
			Expect(var1.Spec.SecretName).To(Equal("foo-deployment.var-foo-cert"))
			request := var1.Spec.Request.CertificateRequest
			Expect(request.CommonName).To(Equal("example.com"))
			Expect(request.AlternativeNames).To(Equal([]string{"foo.com", "bar.com"}))
			Expect(request.IsCA).To(Equal(true))
			Expect(request.CARef.Name).To(Equal("foo-deployment.var-theca"))
			Expect(request.CARef.Key).To(Equal("certificate"))
		})
	})

	Context("when the lifecycle is set to service", func() {
		It("converts the instance group to an ExtendedStatefulset", func() {
			kubeConfig, err := m.ConvertToKube("foo")
			Expect(err).ShouldNot(HaveOccurred())
			anExtendedSts := kubeConfig.InstanceGroups[0].Spec.Template.Spec.Template
			Expect(anExtendedSts.Name).To(Equal("diego-cell"))

			specCopierInitContainer := anExtendedSts.Spec.InitContainers[0]
			rendererInitContainer := anExtendedSts.Spec.InitContainers[1]

			// Test containers in the extended statefulset
			Expect(anExtendedSts.Spec.Containers[0].Image).To(Equal("hub.docker.com/cfcontainerization/cflinuxfs3:opensuse-15.0-28.g837c5b3-30.263-7.0.0_233.gde0accd0-0.62.0"))
			Expect(anExtendedSts.Spec.Containers[0].Command[0]).To(Equal("bash"))
			Expect(anExtendedSts.Spec.Containers[0].Name).To(Equal("job-cflinuxfs3-rootfs-setup"))

			// Test init containers in the extended statefulset
			Expect(specCopierInitContainer.Image).To(Equal("hub.docker.com/cfcontainerization/cflinuxfs3:opensuse-15.0-28.g837c5b3-30.263-7.0.0_233.gde0accd0-0.62.0"))
			Expect(specCopierInitContainer.Command[0]).To(Equal("bash"))
			Expect(specCopierInitContainer.Name).To(Equal("spec-copier-cflinuxfs3-rootfs-setup"))
			Expect(rendererInitContainer.Image).To(Equal("/:"))
			Expect(rendererInitContainer.Name).To(Equal("renderer-diego-cell"))

			// Test shared volume setup
			Expect(anExtendedSts.Spec.Containers[0].VolumeMounts[0].Name).To(Equal("rendering-data"))
			Expect(anExtendedSts.Spec.Containers[0].VolumeMounts[0].MountPath).To(Equal("/var/vcap/rendering"))
			Expect(specCopierInitContainer.VolumeMounts[0].Name).To(Equal("rendering-data"))
			Expect(specCopierInitContainer.VolumeMounts[0].MountPath).To(Equal("/var/vcap/rendering"))
			Expect(rendererInitContainer.VolumeMounts[0].Name).To(Equal("rendering-data"))
			Expect(rendererInitContainer.VolumeMounts[0].MountPath).To(Equal("/var/vcap/rendering"))

			// Test the renderer container setup
			Expect(rendererInitContainer.Env[0].Name).To(Equal("INSTANCE_GROUP_NAME"))
			Expect(rendererInitContainer.Env[0].Value).To(Equal("diego-cell"))
			Expect(rendererInitContainer.VolumeMounts[1].Name).To(Equal("manifest"))
			Expect(rendererInitContainer.VolumeMounts[1].MountPath).To(Equal("/var/vcap/rendering-manifest"))
		})
	})

	Context("when the lifecycle is set to errand", func() {
		It("converts the instance group to an ExtendedJob", func() {
			kubeConfig, err := m.ConvertToKube("foo")
			Expect(err).ShouldNot(HaveOccurred())
			anExtendedJob := kubeConfig.Errands[0]

			Expect(len(kubeConfig.Errands)).To(Equal(1))
			Expect(len(kubeConfig.Errands)).ToNot(Equal(2))
			Expect(anExtendedJob.Name).To(Equal("foo-deployment-redis-slave"))

			specCopierInitContainer := anExtendedJob.Spec.Template.Spec.InitContainers[0]
			rendererInitContainer := anExtendedJob.Spec.Template.Spec.InitContainers[1]

			// Test containers in the extended job
			Expect(anExtendedJob.Spec.Template.Spec.Containers[0].Name).To(Equal("job-redis-server"))
			Expect(anExtendedJob.Spec.Template.Spec.Containers[0].Image).To(Equal("hub.docker.com/cfcontainerization/redis:opensuse-42.3-28.g837c5b3-30.263-7.0.0_234.gcd7d1132-36.15.0"))
			Expect(anExtendedJob.Spec.Template.Spec.Containers[0].Command[0]).To(Equal("bash"))

			// Test init containers in the extended job
			Expect(specCopierInitContainer.Image).To(Equal("hub.docker.com/cfcontainerization/redis:opensuse-42.3-28.g837c5b3-30.263-7.0.0_234.gcd7d1132-36.15.0"))
			Expect(specCopierInitContainer.Command[0]).To(Equal("bash"))
			Expect(rendererInitContainer.Image).To(Equal("/:"))
			Expect(rendererInitContainer.Name).To(Equal("renderer-redis-slave"))

			// Test shared volume setup
			Expect(anExtendedJob.Spec.Template.Spec.Containers[0].VolumeMounts[0].Name).To(Equal("rendering-data"))
			Expect(anExtendedJob.Spec.Template.Spec.Containers[0].VolumeMounts[0].MountPath).To(Equal("/var/vcap/rendering"))
			Expect(specCopierInitContainer.VolumeMounts[0].Name).To(Equal("rendering-data"))
			Expect(specCopierInitContainer.VolumeMounts[0].MountPath).To(Equal("/var/vcap/rendering"))
			Expect(rendererInitContainer.VolumeMounts[0].Name).To(Equal("rendering-data"))
			Expect(rendererInitContainer.VolumeMounts[0].MountPath).To(Equal("/var/vcap/rendering"))

			// Test mounting the resolved instance group properties in the renderer container
			Expect(rendererInitContainer.Env[0].Name).To(Equal("INSTANCE_GROUP_NAME"))
			Expect(rendererInitContainer.Env[0].Value).To(Equal("redis-slave"))
			Expect(rendererInitContainer.VolumeMounts[1].Name).To(Equal("manifest"))
			Expect(rendererInitContainer.VolumeMounts[1].MountPath).To(Equal("/var/vcap/rendering-manifest"))
		})
	})

	Context("GetReleaseImage", func() {
		It("reports an error if the instance group was not found", func() {
			_, err := m.GetReleaseImage("unknown-instancegroup", "redis-server")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not found"))
		})

		It("reports an error if the stemcell was not found", func() {
			m.Stemcells = []*manifest.Stemcell{}
			_, err := m.GetReleaseImage("redis-slave", "redis-server")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("stemcell could not be resolved"))
		})

		It("reports an error if the job was not found", func() {
			_, err := m.GetReleaseImage("redis-slave", "unknown-job")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not found"))
		})

		It("reports an error if the release was not found", func() {
			m.Releases = []*manifest.Release{}
			_, err := m.GetReleaseImage("redis-slave", "redis-server")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not found"))
		})

		It("calculates the release image name", func() {
			releaseImage, err := m.GetReleaseImage("redis-slave", "redis-server")
			Expect(err).ToNot(HaveOccurred())
			Expect(releaseImage).To(Equal("hub.docker.com/cfcontainerization/redis:opensuse-42.3-28.g837c5b3-30.263-7.0.0_234.gcd7d1132-36.15.0"))
		})

		It("uses the release stemcell information if it is set", func() {
			releaseImage, err := m.GetReleaseImage("diego-cell", "cflinuxfs3-rootfs-setup")
			Expect(err).ToNot(HaveOccurred())
			Expect(releaseImage).To(Equal("hub.docker.com/cfcontainerization/cflinuxfs3:opensuse-15.0-28.g837c5b3-30.263-7.0.0_233.gde0accd0-0.62.0"))
		})
	})
})
