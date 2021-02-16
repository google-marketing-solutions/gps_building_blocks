// Workflow that labels a candidate's MPM packages and tags the candidate.
//
// Accepts this required workflow parameter:
//
// *   environment: Target environment, the name of a DeploymentEnvironmentInfo
//     entry in your blueprint. The workflow will apply the mpm_label for that
//     environment to the candidate's packages. Defaults to 'live'.
//
// The workflow does the following:
//
// 1.  Tags the candidate with ReleaseType=production if environment is 'live'.
// 2.  Labels the packages built by the candidate based on the mpm_label of the
//     DeploymentEnvironmentInfo whose name matches the given environment.
// 3.  Tags the candidate with the name of the given environment.

// http://google3/releasetools/rapid/tasks/executors/kokoro/trigger_build.py

import '//releasetools/rapid/workflows/rapid.pp' as rapid

vars = rapid.create_vars() {}

task_deps = [
  'kokoro.trigger_build':['start']
]

task_properties = [
  'kokoro.trigger_build' : [
    "full_job_name=prod:gps_building_blocks/gcp_ubuntu/release",
    "changelist=head",
    "wait_for_build=true",
    "env_vars=RELEASE_TYPE=prod"
  ]
]

workflow label_packages = rapid.workflow([task_deps, task_properties]) {
  vars = @vars
}
