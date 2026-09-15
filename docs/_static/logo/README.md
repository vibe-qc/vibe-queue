# Brand assets

Hand-written SVG, not generated raster. The wordmark's lettering is drawn as
geometric primitives rather than type, so it needs no font at render time and
stays crisp at any size.

| File | Used by |
|---|---|
| `vq-favicon.svg` | `html_favicon` in `docs/conf.py`; the browser-tab icon |
| `vq-wordmark-light.svg` | `light_logo`; the docs sidebar in light mode |
| `vq-wordmark-dark.svg` | `dark_logo`; the docs sidebar in dark mode |
| `vq-social.svg` | Source for the card below |
| `vq-social.png` | `og:image` / `twitter:image` in `docs/index.md` front matter |

## The glyph

Three tokens advancing along a rail, inside the same rounded unit-cell frame
vibe-qc uses, in the same teal (`#0F766E`), so the two products read as one
family. vq's own vocabulary fills the frame: vibe-qc puts a wavefunction
crossing a unit cell there, vq puts a queue.

The tokens are **solid with graded opacity**, not outlined. At 16 px a 2 px
stroke on a 5 px box collides with its neighbour and the row blurs into a
single blob; opacity survives the downscale, strokes do not.

The glyph is duplicated into all four files, because inlining keeps each one
standalone. `tests/test_logo_assets.py` asserts the copies have not drifted:
edit one and it names the others.

## Regenerating the PNG

`og:image` must be a raster; social platforms do not reliably render SVG. The
committed PNG was produced with headless Chrome, which respects the SVG's
`viewBox` and font stacks:

```sh
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --headless --disable-gpu --screenshot=docs/_static/logo/vq-social.png \
  --window-size=1200,630 --hide-scrollbars \
  "file://$PWD/docs/_static/logo/vq-social.svg"
```

Do not use `qlmanage`: it pads the output to a square, which silently letterboxes
the card.

Re-run this whenever `vq-social.svg` changes. Nothing checks that the two are in
sync, because the PNG is a build product of a file that changes rarely; if that
stops being true, add the check rather than relying on remembering.
