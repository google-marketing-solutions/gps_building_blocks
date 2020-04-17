# gPS Solutions - Core Libraries Repository

This repository contains modules and tools useful for use with advanced data
solutions on Google Ads, Google Marketing Platform and Google Cloud. It is
maintained by a team of Customer Solutions Engineers (CSE) and Data Scientists
(DS) working within Google's Technical Solutions (gTech) Professional Services
organization (gPS).

Contributions are highly encouraged; see [CONTRIBUTING.md](CONTRIBUTING.md).

**This is not an official Google product.**
<!-- BEGIN GOOGLE-INTERNAL -->

## Instructions for gPS Building Blocks Internal Contributors

This repository utilizes [Copybara](http://go/copybara){target="_blank"} to sync
its state between google3, Git-on-Borg and GitHub (repo links can be found
[below](#see-also)). As a result, a few things need to be taken into
consideration to allow copybara to work properly.

**Note:** For more information on configuring copybara and its different
workflows, refer to the documentation at go/gPS-BB-copybara.

### Commit Messages

Wrap the CL description you would like to appear on Git-on-Borg and/or GitHub
with the tags: `BEGIN_PUBLIC` and `END_PUBLIC`. Alternatively, use `PUBLIC:` for
single-line comments. If you do not add the OSS tags properly the commit message
will be exported as `Internal Change`. For more information, refer to the
copybara documentation
[here](http://go/copybara-library/scrubbing#message_extract_public_description.arguments){target="_blank"}.

### Source Code Files

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

### Usage of google3 Libraries

Please be careful when _explicitly_ referencing internal google3 libraries in
your python files (e.g. `from google3.xxx` or `import google3.xxx`), since these
libraries may not be publicly available. There are primarily two ways of
mitigating this:

*   Modify your BUILD file so that google3 dependencies are _implicitly_
    referenced (e.g. `import google3.third_party.py.googleapiclient` becomes
    `import googleapiclient`). See go/py-strict-deps for more information
*   Explicitly add a _transform_ element to the *py_transforms* array in the
    [copybara configuration file](copy.bara.sky) to convert the internal library
    path into its external-friendly one

### Documentation Files

As with commit messages, you can wrap internal documentation in `*.md` files by
using the following tags:

`<!-- BEGIN GOOGLE-INTERNAL -->`

Any documentation here (including the wrapper tags) will be removed when
exporting to GoB and/or GitHub.

`<!-- END GOOGLE-INTERNAL -->`
<!-- BEGIN GOOGLE-INTERNAL -->

**Note:** Please beware when approving public contributions (CLs based on GitHub
PRs) for `*.md` files that contain internal documentation, since all internal
documentation will effectively be removed. This is due to the fact that the
[copybara transformation](https://source.corp.google.com/piper///depot/google3/third_party/google_research/google/copybara/oss.bara.sky;l=94)
responsible for scrubbing internal documentation before exporting code to
external sources is [irreversible](http://go/copybara-reference#core.transform).
It is best if you correct any unintentionally modified files directly during the
copybara safe review step on Gerrit first before approving the change there, so
that created CLs are already pruned before reviewal in Critique.

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

<!-- END GOOGLE-INTERNAL -->
