package cmd

import (
	"bufio"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/pkg/errors"

	"code.cloudfoundry.org/cf-operator/pkg/bosh/bpm"
	"code.cloudfoundry.org/cf-operator/pkg/bosh/manifest"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	btg "github.com/viovanov/bosh-template-go"
	yaml "gopkg.in/yaml.v2"
)

// dataGatherCmd represents the dataGather command
var dataGatherCmd = &cobra.Command{
	Use:   "data-gather [flags]",
	Short: "Gathers data of a bosh manifest",
	Long: `Gathers data of a manifest. 

This will retrieve information of an instance-group
inside a bosh manifest.

`,
	RunE: func(cmd *cobra.Command, args []string) error {
		mFile := viper.GetString("bosh_manifest")
		if len(mFile) == 0 {
			return fmt.Errorf("manifest cannot be empty")
		}

		baseDir := viper.GetString("base_dir")
		if len(baseDir) == 0 {
			return fmt.Errorf("base directory cannot be empty")
		}

		ns := viper.GetString("kubernetes_namespace")
		if len(ns) == 0 {
			return fmt.Errorf("namespace cannot be empty")
		}

		instanceGroupName := viper.GetString("instance_group")

		mBytes, err := ioutil.ReadFile(mFile)
		if err != nil {
			return err
		}

		mStruct := manifest.Manifest{}
		err = yaml.Unmarshal(mBytes, &mStruct)
		if err != nil {
			return err
		}

		result, err := GatherData(&mStruct, baseDir, ns, instanceGroupName)
		if err != nil {
			return err
		}

		jsonBytes, err := json.Marshal(map[string]string{
			"properties.yaml": string(result),
		})
		if err != nil {
			return errors.Wrapf(err, "could not marshal json output")
		}

		f := bufio.NewWriter(os.Stdout)
		defer f.Flush()
		_, err = f.Write(jsonBytes)
		if err != nil {
			return err
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(dataGatherCmd)

	dataGatherCmd.Flags().StringP("manifest", "m", "", "path to a bosh manifest")
	//TODO: can we reuse the global ns flag
	dataGatherCmd.Flags().String("kubernetes-namespace", "", "the kubernetes namespace")
	dataGatherCmd.Flags().StringP("base-dir", "b", "", "a path to the base directory")
	dataGatherCmd.Flags().StringP("instance-group", "g", "", "instance group for data gathering")

	// This will get the values from any set ENV var, but always
	// the values provided via the flags have more precedence.
	viper.AutomaticEnv()
	viper.BindPFlag("bosh_manifest", dataGatherCmd.Flags().Lookup("manifest"))
	viper.BindPFlag("kubernetes_namespace", dataGatherCmd.Flags().Lookup("kubernetes-namespace"))
	viper.BindPFlag("base_dir", dataGatherCmd.Flags().Lookup("base-dir"))
	viper.BindPFlag("instance_group", dataGatherCmd.Flags().Lookup("instance-group"))
}

// CollectReleaseSpecsAndProviderLinks will collect all release specs and bosh links for provider jobs
func CollectReleaseSpecsAndProviderLinks(mStruct *manifest.Manifest, baseDir string, namespace string) (map[string]map[string]manifest.JobSpec, map[string]map[string]manifest.JobLink, error) {
	// Contains YAML.load('.../release_name/job_name/job.MF')
	jobReleaseSpecs := map[string]map[string]manifest.JobSpec{}

	// Lists every link provided by the job
	jobProviderLinks := map[string]map[string]manifest.JobLink{}

	for _, instanceGroup := range mStruct.InstanceGroups {
		for jobIdx, job := range instanceGroup.Jobs {
			// make sure a map entry exists for the current job release
			if _, ok := jobReleaseSpecs[job.Release]; !ok {
				jobReleaseSpecs[job.Release] = map[string]manifest.JobSpec{}
			}

			// load job.MF into jobReleaseSpecs[job.Release][job.Name]
			if _, ok := jobReleaseSpecs[job.Release][job.Name]; !ok {
				jobMFFilePath := filepath.Join(baseDir, "jobs-src", job.Release, job.Name, "job.MF")
				jobMfBytes, err := ioutil.ReadFile(jobMFFilePath)
				if err != nil {
					return nil, nil, err
				}

				jobSpec := manifest.JobSpec{}
				if err := yaml.Unmarshal([]byte(jobMfBytes), &jobSpec); err != nil {
					return nil, nil, err
				}
				jobReleaseSpecs[job.Release][job.Name] = jobSpec
			}

			// spec of the current jobs release/name
			spec := jobReleaseSpecs[job.Release][job.Name]

			// Generate instance spec for each ig instance
			// This will be stored inside the current job under
			// job.properties.bosh_containerization
			var jobsInstances []manifest.JobInstance
			for i := 0; i < instanceGroup.Instances; i++ {

				// TODO: Understand whether there are negative side-effects to using this
				// default
				azs := []string{""}
				if len(instanceGroup.Azs) > 0 {
					azs = instanceGroup.Azs
				}

				for _, az := range azs {
					index := len(jobsInstances)
					name := fmt.Sprintf("%s-%s", instanceGroup.Name, job.Name)
					id := fmt.Sprintf("%v-%v-%v", instanceGroup.Name, index, job.Name)
					// TODO: not allowed to hardcode svc.cluster.local
					address := fmt.Sprintf("%s.%s.svc.cluster.local", id, namespace)

					jobsInstances = append(jobsInstances, manifest.JobInstance{
						Address:  address,
						AZ:       az,
						ID:       id,
						Index:    index,
						Instance: i,
						Name:     name,
					})
				}
			}

			// set jobs.properties.bosh_containerization.instances with the ig instances
			instanceGroup.Jobs[jobIdx].Properties.BOSHContainerization.Instances = jobsInstances

			// Create a list of fully evaluated links provided by the current job
			// These is specified in the job release job.MF file
			if spec.Provides != nil {
				var properties map[string]interface{}

				for _, provider := range spec.Provides {
					properties = map[string]interface{}{}
					for _, property := range provider.Properties {
						// generate a nested struct of map[string]interface{} when
						// a property is of the form foo.bar
						if strings.Contains(property, ".") {
							propertyStruct := RetrieveNestedProperty(spec, property)
							properties = propertyStruct
						} else {
							properties[property] = RetrievePropertyDefault(spec, property)
						}
					}
					// Override default spec values with explicit settings from the
					// current bosh deployment manifest, this should be done under each
					// job, inside a `properties` key.
					for propertyName := range properties {
						if explicitSetting, ok := LookUpProperty(job, propertyName); ok {
							properties[propertyName] = explicitSetting
						}
					}
					providerName := provider.Name
					providerType := provider.Type

					// instance_group.job can override the link name through the
					// instance_group.job.provides, via the "as" key
					if instanceGroup.Jobs[jobIdx].Provides != nil {
						if value, ok := instanceGroup.Jobs[jobIdx].Provides[providerName]; ok {
							switch value.(type) {
							case map[interface{}]interface{}:
								if overrideLinkName, ok := value.(map[interface{}]interface{})["as"]; ok {
									providerName = fmt.Sprintf("%v", overrideLinkName)
								}
							default:
								return nil, nil, fmt.Errorf("unexpected type detected: %T, should have been a map", value)
							}

						}
					}

					if providers, ok := jobProviderLinks[providerType]; ok {
						if _, ok := providers[providerName]; ok {
							return nil, nil, fmt.Errorf("multiple providers for link: name=%s type=%s", providerName, providerType)
						}
					}

					if _, ok := jobProviderLinks[providerType]; !ok {
						jobProviderLinks[providerType] = map[string]manifest.JobLink{}
					}

					// construct the jobProviderLinks of the current job that provides
					// a link
					jobProviderLinks[providerType][providerName] = manifest.JobLink{
						Instances:  jobsInstances,
						Properties: properties,
					}
				}
			}
		}
	}

	return jobReleaseSpecs, jobProviderLinks, nil
}

// GenerateJobConsumersData will populate a job with its corresponding provider links
// under properties.bosh_containerization.consumes
func GenerateJobConsumersData(currentJob *manifest.Job, jobReleaseSpecs map[string]map[string]manifest.JobSpec, jobProviderLinks map[string]map[string]manifest.JobLink) error {
	currentJobSpecData := jobReleaseSpecs[currentJob.Release][currentJob.Name]
	for _, consumes := range currentJobSpecData.Consumes {

		consumesName := consumes.Name

		if currentJob.Consumes != nil {
			// Deployment manifest can intentionally prevent link resolution as long as the link is optional
			// Continue to the next job if this one does not consumes links
			if _, ok := currentJob.Consumes[consumesName]; !ok {
				if consumes.Optional {
					continue
				}
				return fmt.Errorf("mandatory link of consumer %s is explicitly set to nil", consumesName)
			}

			// When the job defines a consumes property in the manifest, use it instead of the one
			// from currentJobSpecData.Consumes
			if _, ok := currentJob.Consumes[consumesName]; ok {
				if value, ok := currentJob.Consumes[consumesName].(map[interface{}]interface{})["from"]; ok {
					consumesName = value.(string)
				}
			}
		}

		link, hasLink := jobProviderLinks[consumes.Type][consumesName]
		if !hasLink && !consumes.Optional {
			return fmt.Errorf("cannot resolve non-optional link for consumer %s", consumesName)
		}

		// generate the job.properties.bosh_containerization.consumes struct with the links information from providers.
		if currentJob.Properties.BOSHContainerization.Consumes == nil {
			currentJob.Properties.BOSHContainerization.Consumes = map[string]manifest.JobLink{}
		}

		currentJob.Properties.BOSHContainerization.Consumes[consumesName] = manifest.JobLink{
			Instances:  link.Instances,
			Properties: link.Properties,
		}
	}
	return nil
}

// RenderJobBPM per job and add its value to the jobInstances.BPM field
func RenderJobBPM(currentJob manifest.Job, jobInstances []manifest.JobInstance, baseDir string, manifestName string) error {

	// Location of the current job job.MF file
	jobSpecFile := filepath.Join(baseDir, "jobs-src", currentJob.Release, currentJob.Name, "job.MF")

	var jobSpec struct {
		Templates map[string]string `yaml:"templates"`
	}

	// First, we must figure out the location of the template.
	// We're looking for a template in the spec, whose result is a file "bpm.yml"
	yamlFile, err := ioutil.ReadFile(jobSpecFile)
	if err != nil {
		return errors.Wrap(err, "failed to read the job spec file")
	}
	err = yaml.Unmarshal(yamlFile, &jobSpec)
	if err != nil {
		return errors.Wrap(err, "failed to unmarshal the job spec file")
	}

	bpmSource := ""
	for srcFile, dstFile := range jobSpec.Templates {
		if filepath.Base(dstFile) == "bpm.yml" {
			bpmSource = srcFile
			break
		}
	}

	if bpmSource == "" {
		return fmt.Errorf("can't find BPM template for job %s", currentJob.Name)
	}

	// ### Render bpm.yml.erb for each job instance

	erbFilePath := filepath.Join(baseDir, "jobs-src", currentJob.Release, currentJob.Name, "templates", bpmSource)
	if _, err := os.Stat(erbFilePath); err != nil {
		return err
	}

	if jobInstances != nil {
		for i, instance := range jobInstances {

			properties := currentJob.Properties.ToMap()

			renderPointer := btg.NewERBRenderer(
				&btg.EvaluationContext{
					Properties: properties,
				},

				&btg.InstanceInfo{
					Address:    instance.Address,
					AZ:         instance.AZ,
					ID:         instance.ID,
					Index:      string(instance.Index),
					Deployment: manifestName,
					Name:       instance.Name,
				},

				jobSpecFile,
			)

			// Would be good if we can write the rendered file into memory,
			// rather than to disk
			tmpfile, err := ioutil.TempFile("", "rendered.*.yml")
			if err != nil {
				return err
			}
			defer os.Remove(tmpfile.Name())

			if err := renderPointer.Render(erbFilePath, tmpfile.Name()); err != nil {
				return err
			}

			bpmBytes, err := ioutil.ReadFile(tmpfile.Name())
			if err != nil {
				return err
			}

			// Parse a rendered bpm.yml into the bpm Config struct
			jobInstances[i].BPM, err = bpm.NewConfig(bpmBytes)
			if err != nil {
				return err
			}

			// Consider adding a Fingerprint to each job instance
			// instance.Fingerprint = generateSHA(fingerPrintBytes)
		}
	}
	return nil
}

// ProcessConsumersAndRenderBPM will generate a proper context for links and render the required ERB files
func ProcessConsumersAndRenderBPM(mStruct *manifest.Manifest, baseDir string, jobReleaseSpecs map[string]map[string]manifest.JobSpec, jobProviderLinks map[string]map[string]manifest.JobLink, instanceGroupName string) ([]byte, error) {
	var desiredInstanceGroup *manifest.InstanceGroup
	for _, instanceGroup := range mStruct.InstanceGroups {
		if instanceGroup.Name != instanceGroupName {
			continue
		}

		desiredInstanceGroup = instanceGroup
		break
	}

	if desiredInstanceGroup == nil {
		return nil, errors.Errorf("can't find instance group '%s' in manifest", instanceGroupName)
	}

	for idJob, job := range desiredInstanceGroup.Jobs {

		currentJob := &desiredInstanceGroup.Jobs[idJob]

		// Verify that the current job release exists on the manifest releases block
		if lookUpJobRelease(mStruct.Releases, job.Release) {
			currentJob.Properties.BOSHContainerization.Release = job.Release
		}

		err := GenerateJobConsumersData(currentJob, jobReleaseSpecs, jobProviderLinks)
		if err != nil {
			return nil, err
		}

		// Get current job.bosh_containerization.instances, which will be required by the renderer to generate
		// the render.InstanceInfo struct
		jobInstances := currentJob.Properties.BOSHContainerization.Instances

		err = RenderJobBPM(*currentJob, jobInstances, baseDir, mStruct.Name)
		if err != nil {
			return nil, err
		}

		// Store shared bpm as a top level property
		if len(jobInstances) < 1 {
			continue
		}

		allBPMEqual := true

		for _, jobInstance := range jobInstances {
			if !reflect.DeepEqual(jobInstance, jobInstances[0].BPM) {
				allBPMEqual = false
				break
			}
		}

		if allBPMEqual {
			// Store shared bpm as a top level property
			job.Properties.BOSHContainerization.BPM = jobInstances[0].BPM

			// Remove all other BPM information
			for _, jobInstance := range jobInstances {
				jobInstance.BPM = bpm.Config{}
			}
		}
	}

	// marshall the whole manifest Structure
	manifestResolved, err := yaml.Marshal(mStruct)
	if err != nil {
		return nil, err
	}

	return manifestResolved, nil
}

// generateSHA will generate a new fingerprint based on
// a struct
func generateSHA(fingerPrint []byte) []byte {
	h := md5.New()
	h.Write(fingerPrint)
	bs := h.Sum(nil)
	return bs
}

// GatherData will collect different data
// Collect job spec information
// Collect job properties
// Collect bosh links
// Render the bpm yaml file data
func GatherData(mStruct *manifest.Manifest, baseDir string, namespace string, instanceGroupName string) ([]byte, error) {
	jobReleaseSpecs, jobProviderLinks, err := CollectReleaseSpecsAndProviderLinks(mStruct, baseDir, namespace)
	if err != nil {
		return nil, err
	}

	return ProcessConsumersAndRenderBPM(mStruct, baseDir, jobReleaseSpecs, jobProviderLinks, instanceGroupName)
}

// LookUpProperty search for property value in the job properties
func LookUpProperty(job manifest.Job, propertyName string) (interface{}, bool) {
	var pointer interface{}

	pointer = job.Properties.Properties
	for _, pathPart := range strings.Split(propertyName, ".") {
		switch pointer.(type) {
		case map[string]interface{}:
			hash := pointer.(map[string]interface{})
			if _, ok := hash[pathPart]; !ok {
				return nil, false
			}
			pointer = hash[pathPart]

		case map[interface{}]interface{}:
			hash := pointer.(map[interface{}]interface{})
			if _, ok := hash[pathPart]; !ok {
				return nil, false
			}
			pointer = hash[pathPart]

		default:
			return nil, false
		}
	}
	return pointer, true
}

// RetrieveNestedProperty will generate an nested struct
// based on a string of the type foo.bar
func RetrieveNestedProperty(jobSpec manifest.JobSpec, propertyName string) map[string]interface{} {
	var anStruct map[string]interface{}
	var previous map[string]interface{}
	items := strings.Split(propertyName, ".")
	for i := len(items) - 1; i >= 0; i-- {
		if i == (len(items) - 1) {
			previous = map[string]interface{}{
				items[i]: RetrievePropertyDefault(jobSpec, propertyName),
			}
		} else {
			anStruct = map[string]interface{}{
				items[i]: previous,
			}
			previous = anStruct

		}
	}
	return anStruct
}

// RetrievePropertyDefault return the default value of the spec property
func RetrievePropertyDefault(jobSpec manifest.JobSpec, propertyName string) interface{} {
	if property, ok := jobSpec.Properties[propertyName]; ok {
		return property.Default
	}

	return nil
}

// lookUpJobRelease will check in the main manifest for
// a release name
func lookUpJobRelease(releases []*manifest.Release, jobRelease string) bool {
	for _, release := range releases {
		if release.Name == jobRelease {
			return true
		}
	}

	return false
}
