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

shell_command = 'borgcfg --user=cse-tools-devops-jobs --skip_confirmation --wait_done /google_src/head/depot/google3/corp/gtech/ads/building_blocks/meta/version_file_updater/jobs/update_version.borg up '

task_deps = [
  'shell-update_version':['start'],
  'kokoro.trigger_build':['shell-update_version']
]

task_properties = [
  'shell-update_version' : ['command=' + shell_command],
  'kokoro.trigger_build' : [
    'full_job_name=gps_building_blocks/gcp_ubuntu/release',
    'changelist=%(build_changelist)s',
    'wait_for_build=true',
    'env_vars=RELEASE_TYPE=prod',
    'multi_scm=True',
  ]
]

workflow label_packages = rapid.workflow([task_deps, task_properties]) {
  vars = @vars
}
