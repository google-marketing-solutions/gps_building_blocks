// Workflow that updates a target VERSION document and triggers a Kokoro build.
//
// Accepts this required workflow parameter:
//
// *   n/a
//
// The workflow does the following:
//
// 1.  Starts a Borg job executing the version_file_updater MPM module
// 2.  Triggers a Kokoro Realease workflow.

// http://google3/releasetools/rapid/tasks/executors/kokoro/trigger_build.py

import '//releasetools/rapid/workflows/rapid.pp' as rapid

vars = rapid.create_vars() {}

shell_command = 'borgcfg /google_src/head/depot/google3/corp/gtech/ads/building_blocks/meta/version_file_updater/jobs/update_version.borg up --user=cse-tools-devops-jobs --skip-confirmation'

task_deps = [
  'shell-update_version':['start'],
  'kokoro.trigger_build':['shell-update_version']
]

task_properties = [
  'shell-update_version' : ['command=' + shell_command],
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