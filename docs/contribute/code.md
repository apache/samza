---
layout: page
title: Code
---

Samza's code is in an Apache Git repository located [here](https://git-wip-us.apache.org/repos/asf?p=incubator-samza.git;a=tree).

You can check out Samza's code by running:

```
git clone http://git-wip-us.apache.org/repos/asf/incubator-samza.git
```

Please see the [Rules](rules.html) page for information on how to contribute.

If you are a committer you need to use https instead of http to check in, otherwise you will get an error regarding an inability to acquire a lock. Note that older versions of git may also give this error even when the repo was cloned with https; if you experience this try a newer version of git.

The Samza website is built by Jekyll from the markdown files found in the docs subdirectory. For committers wishing to update the webpage first install Jekyll:

```
gem install jekyll
```

Depending on your system you may also need install some additional dependencies when you try and run it. Note that some Linux distributions may have older versions of Jekyll packaged that treat arguments differently and may result in changes not being incorporated into the generated site.

The script to commit the updated webpage files is docs/_tools/publish-site.sh
