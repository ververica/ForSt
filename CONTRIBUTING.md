# Contributing to ForSt

## Code of Conduct
The code of conduct is described in [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md)

## Basic Development Workflow
As most open-source projects in github, ForSt contributors work on their forks, and send pull requests to ForSt’s repo. After a reviewer approves the pull request and all the CI check are passed, a ForSt team member will merge it.

## Code style
ForSt follows the RocksDB's code format.
RocksDB follows Google C++ Style: https://google.github.io/styleguide/cppguide.html
Note: a common pattern in existing RocksDB code is using non-nullable Type* for output parameters, in the old Google C++ Style, but this guideline has changed. The new guideline prefers (non-const) references for output parameters.
For formatting, we limit each line to 80 characters. Most formatting can be done automatically by running
```
build_tools/format-diff.sh
```
or simply ```make format``` if you use GNU make. If you lack of dependencies to run it, the script will print out instructions for you to install them.


## License Claim
ForSt is licensed under Apache 2.0 License. But since the RocksDB has its own license, we keep the license claim on top of each existing files, and use/add Apache 2.0 License on top of each new created files.
```
/* Copyright 2024-present, the ForSt authors. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
```

## Submit patches
Before you submit a patch, we strongly recommend that you share your ideas with others
in the community via [Issues](https://github.com/ververica/ForSt/issues) or 
[Discussions](https://github.com/ververica/ForSt/discussions). Of course, you do not
need to do this if you are submitting a patch that can already be associated with an 
issue, or a minor patch like a typo fix. You can then submit your patch via 
[Pull Requests](https://github.com/ververica/ForSt/pulls), which requires a GitHub account.
