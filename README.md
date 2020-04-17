# gPS Solutions - Core Libraries Repository

go/gPS-BB-doc

<!--*
# Document freshness: For more information, see go/fresh-source.
freshness: { owner: 'mfekry' reviewed: '2020-04-17' }
*-->

**Note:** This is the internal documentation for the gps_building_blocks
repository and should be used for any content that we do not want to publicize.
The external documentation can be found [here](EXTERNAL.md)
and should only contain external-friendly information pertaining to the
repository contents.

This repository utilizes [Copybara](http://go/copybara){target="_blank"} to sync
its state between google3, Git-on-Borg and GitHub (repo links can be found
[below](#see-also)). As a result, a few things need to be taken into
consideration to allow copybara to work properly. The following sections provide
instructions for gPS BB internal contributors.

**Note:** For more information on configuring copybara and its different
workflows, refer to the documentation at go/gPS-BB-copybara.

## Commit Messages

Wrap the CL description you would like to appear on Git-on-Borg and/or GitHub
with the tags: `BEGIN_PUBLIC` and `END_PUBLIC`. Alternatively, use `PUBLIC:` for
single-line comments. If you do not add the OSS tags properly the commit message
will be exported as `Internal Change`. For more information, refer to the
copybara documentation
[here](http://go/copybara-library/scrubbing#message_extract_public_description.arguments){target="_blank"}.

## Source Code Files

All source code files must start with the correct license agreement. This is one
of the OSS requirements imposed by Google (see
go/releasing/preparing#license-headers). Please use the
[addlicense](https://github.com/google/addlicense){target="_blank"} tool for
including the license header. Here is an example for adding the *Apache 2.0*
license to all source code files in the current directory: <br/>
`go get -u github.com/google/addlicense && ~/go/bin/addlicense -l apache .`

**Note:** This command is idempotent; it will not add the license to files that
already have it. Beware however since it will **not** take into consideration
any excluded files listed in the copybara configuration. So make sure you review
all modified files after running the script and revert anyundesired chages.

## Usage of google3 Libraries

Please be careful when _explicitly_ referencing internal google3 libraries in
your python files (e.g. `from google3.xxx` or `import google3.xxx`), since these
libraries may not be publicly available. There are primarily two ways of
mitigating this:

*   Modify your BUILD file so that google3 dependencies are _implicitly_
    referenced (e.g. `import google3.third_party.py.googleapiclient` becomes
    `import googleapiclient`). See go/py-strict-deps for more information
*   Explicitly add a _transform_ element to the *py_transforms* array in the
    [copybara configuration file](copy.bara.sky)
    to convert the internal library path into its external-friendly one

### Rejecting Public Contributions

If you would like to reject a public contribution, consider commenting directly
on the GitHub PR and/or closing the PR altogether. You can also _abandon_ the
automatically generated change in the Gerrit safe review. If the Gerrit change
had already been approved and a resulting CL was created in Piper, you will not
be able to _revert_ the CL, since it is owned by the copybara worker. However,
such CLs will automatically be garbage collected by the copybara worker if they
have not been updated for a week (see reference documentation
[here](http://g3doc/devtools/copybara/g3doc/service/piper_cls.md#following-the-state-of-a-cl)).

## See Also

*   The Building Blocks Project also has a directory in
    [//corp/gtech/ads](https://source.corp.google.com/piper///depot/google3/corp/gtech/ads/building_blocks/)
    which is intended to be internal to the CSE/DS team
*   [Buganizer Component](https://b.corp.google.com/issues?q=componentid:828030%20status:open)
    for filing issues, etc.
*   Git-on-Borg
    [repository](https://cse.googlesource.com/common/gps_building_blocks)
    {target="_blank"}
*   GitHub [repository](https://github.com/google/gps_building_blocks)
    {target="_blank"}
