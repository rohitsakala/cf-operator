package manifest

import (
	"fmt"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	bdv1 "code.cloudfoundry.org/cf-operator/pkg/kube/apis/boshdeployment/v1alpha1"
	ejv1 "code.cloudfoundry.org/cf-operator/pkg/kube/apis/extendedjob/v1alpha1"
)

// variableInterpolationJob returns an extended job to interpolate variables
func (m *Manifest) variableInterpolationJob(namespace string) (*ejv1.ExtendedJob, error) {
	cmd := []string{"/bin/sh"}
	args := []string{"-c", `cf-operator variable-interpolation | base64 | tr -d '\n' | echo "{\"interpolated-manifest.yaml\":\"$(</dev/stdin)\"}"`}

	// This is the source manifest, that still has the '((vars))'
	manifestSecretName := m.CalculateSecretName(DeploymentSecretTypeManifestWithOps, "")

	// Prepare Volumes and Volume mounts

	// This is a volume for the "not interpolated" manifest,
	// that has the ops files applied, but still contains '((vars))'
	volumes := []v1.Volume{
		{
			Name: generateVolumeName(manifestSecretName),
			VolumeSource: v1.VolumeSource{
				Secret: &v1.SecretVolumeSource{
					SecretName: manifestSecretName,
				},
			},
		},
	}
	// Volume mount for the manifest
	volumeMounts := []v1.VolumeMount{
		{
			Name:      generateVolumeName(manifestSecretName),
			MountPath: "/var/run/secrets",
			ReadOnly:  true,
		},
	}

	// We need a volume and a mount for each input variable
	for _, variable := range m.Variables {
		varName := variable.Name
		varSecretName := m.CalculateSecretName(DeploymentSecretTypeGeneratedVariable, varName)

		// The volume definition
		vol := v1.Volume{
			Name: generateVolumeName(varSecretName),
			VolumeSource: v1.VolumeSource{
				Secret: &v1.SecretVolumeSource{
					SecretName: varSecretName,
				},
			},
		}
		volumes = append(volumes, vol)

		// And the volume mount
		volMount := v1.VolumeMount{
			Name:      generateVolumeName(varSecretName),
			MountPath: "/var/run/secrets/variables/" + varName,
			ReadOnly:  true,
		}
		volumeMounts = append(volumeMounts, volMount)
	}

	// Calculate the signature of the manifest, to label things
	manifestSignature, err := m.SHA1()
	if err != nil {
		return nil, errors.Wrap(err, "could not calculate manifest SHA1")
	}

	outputSecretPrefix, _ := m.CalculateEJobOutputSecretPrefixAndName(DeploymentSecretTypeManifestAndVars, VarInterpolationContainerName)

	// Assemble the Extended Job
	job := &ejv1.ExtendedJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "variables-interpolation-job",
			Namespace: namespace,
			Labels: map[string]string{
				bdv1.LabelKind:       "variable-interpolation",
				bdv1.LabelDeployment: m.Name,
			},
		},
		Spec: ejv1.ExtendedJobSpec{
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					RestartPolicy: v1.RestartPolicyOnFailure,
					Containers: []v1.Container{
						{
							Name:         VarInterpolationContainerName,
							Image:        GetOperatorDockerImage(),
							Command:      cmd,
							Args:         args,
							VolumeMounts: volumeMounts,
							Env: []v1.EnvVar{
								v1.EnvVar{
									Name:  "MANIFEST",
									Value: "/var/run/secrets/manifest.yaml",
								},
								v1.EnvVar{
									Name:  "VARIABLES_DIR",
									Value: "/var/run/secrets/variables/",
								},
							},
						},
					},
					Volumes: volumes,
				},
			},
			Output: &ejv1.Output{
				NamePrefix: outputSecretPrefix,
				SecretLabels: map[string]string{
					bdv1.LabelKind:         "desired-manifest",
					bdv1.LabelDeployment:   m.Name,
					bdv1.LabelManifestSHA1: manifestSignature,
				},
			},
			Trigger: ejv1.Trigger{
				Strategy: ejv1.TriggerOnce,
			},
		},
	}
	return job, nil
}

// dataGatheringJob generates the Data Gathering Job for a manifest
func (m *Manifest) dataGatheringJob(namespace string) (*ejv1.ExtendedJob, error) {

	_, interpolatedManifestSecretName := m.CalculateEJobOutputSecretPrefixAndName(DeploymentSecretTypeManifestAndVars, VarInterpolationContainerName)

	eJobName := fmt.Sprintf("data-gathering-%s", m.Name)
	outputSecretNamePrefix, _ := m.CalculateEJobOutputSecretPrefixAndName(DeploymentSecretTypeInstanceGroupResolvedProperties, "")

	initContainers := []v1.Container{}
	containers := make([]v1.Container, len(m.InstanceGroups))

	doneSpecCopyingReleases := map[string]bool{}

	for idx, ig := range m.InstanceGroups {

		// Iterate through each Job to find all releases so we can copy all
		// sources to /var/vcap/data-gathering
		for _, boshJob := range ig.Jobs {
			// If we've already generated an init container for this release, skip
			releaseName := boshJob.Release
			if _, ok := doneSpecCopyingReleases[releaseName]; ok {
				continue
			}
			doneSpecCopyingReleases[releaseName] = true

			// Get the docker image for the release
			releaseImage, err := m.GetReleaseImage(ig.Name, boshJob.Name)
			if err != nil {
				return nil, errors.Wrap(err, "failed to calculate release image for data gathering")
			}

			// Create an init container that copies sources
			// TODO: destination should also contain release name, to prevent overwrites
			initContainers = append(initContainers, v1.Container{
				Name:  fmt.Sprintf("spec-copier-%s", releaseName),
				Image: releaseImage,
				VolumeMounts: []v1.VolumeMount{
					v1.VolumeMount{Name: "data-gathering", MountPath: "/var/vcap/data-gathering"},
				},
				Command: []string{"bash", "-c", "cp -ar /var/vcap/jobs-src /var/vcap/data-gathering"},
			})
		}

		// One container per Instance Group
		// There will be one secret generated for each of these containers
		containers[idx] = v1.Container{
			Name:    ig.Name,
			Image:   GetOperatorDockerImage(),
			Command: []string{"/bin/sh"},
			Args:    []string{"-c", `cf-operator data-gather | base64 | tr -d '\n' | echo "{\"properties.yaml\":\"$(</dev/stdin)\"}"`},
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      generateVolumeName(interpolatedManifestSecretName),
					MountPath: "/var/run/secrets",
					ReadOnly:  true,
				},
			},
			Env: []v1.EnvVar{
				v1.EnvVar{
					Name:  "BOSH_MANIFEST",
					Value: "/var/run/secrets/manifest.yml",
				},
				v1.EnvVar{
					Name:  "KUBERNETES_NAMESPACE",
					Value: namespace,
				},
				v1.EnvVar{
					Name:  "BASE_DIR",
					Value: "/var/vcap/data-gathering",
				},
				v1.EnvVar{
					Name:  "INSTANCE_GROUP",
					Value: ig.Name,
				},
			},
		}
	}

	// Construct the data gathering job
	dataGatheringJob := &ejv1.ExtendedJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      eJobName,
			Namespace: namespace,
		},
		Spec: ejv1.ExtendedJobSpec{
			Output: &ejv1.Output{
				NamePrefix: outputSecretNamePrefix,
				SecretLabels: map[string]string{
					LabelDeploymentName: m.Name,
				},
			},
			UpdateOnConfigChange: true,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name: eJobName,
				},
				Spec: v1.PodSpec{
					// Init Container to copy contents
					InitContainers: initContainers,
					// Container to run data gathering
					Containers: containers,
					// Volumes for secrets
					Volumes: []v1.Volume{
						{
							Name: generateVolumeName(interpolatedManifestSecretName),
							VolumeSource: v1.VolumeSource{
								Secret: &v1.SecretVolumeSource{
									SecretName: interpolatedManifestSecretName,
								},
							},
						},
					},
				},
			},
		},
	}

	return dataGatheringJob, nil
}
