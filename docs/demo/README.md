# Demo GIF recording

Rebuild `docs/assets/ifetch-demo.gif` (fixture-backed first-run path):

```sh
PATH="$HOME/go/bin:$PATH" xvfb-run -a vhs docs/demo/demo.tape
```

`fixture_run.py` drives the real DownloadManager package-expand and metadata
fast-path code against in-process fixtures — no live Apple ID required.
