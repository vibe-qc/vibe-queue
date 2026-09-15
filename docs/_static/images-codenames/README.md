# Release codename artwork

One 1672 x 941 PNG per **minor** release, named
`NN-vq-vX.Y.0-slug.png`. Patch releases inherit their parent minor's image,
the same way they inherit its codename.

`prompts.json` is the generation manifest: version, name, file, the exact
prompt used, theme, treatment, status, generator and canvas. Record a prompt
here when its image is generated, so a later regeneration starts from what
actually produced the picture rather than from a reconstruction.

The house style, the reasoning behind it, and paste-ready prompts for every
release are in
[`.release-status/IMAGE-BRIEF-codename-series.md`](../../../.release-status/IMAGE-BRIEF-codename-series.md).

The complete 27-image series covers v0.7.0 through v0.33.0. The seven entries
from v0.27.0 onward are provisional and must be re-confirmed at release time.

The approved v0.9.0 **Tukey's Window** image is the physical-design reference
for all 26 other renders. `prompt` preserves the brief verbatim;
`generation_prompt` records the exact full request, including the reference
instruction. `style_reference` identifies the reference PNG, and
`refinement_prompts` records ordered follow-up edits when needed. The anchor
also retains its original `edit_prompt`. `source_canvas` and `resize` record
whether a full-frame resize was required; images are never cropped.

`alt` describes the actual picture, and `caption` connects it to the release
or provisional concept. Images appear in [the gallery](../../codenames.md)
and [version compatibility](../../version_compatibility.md). Keep those
embeds and the manifest synchronized when adding or revising artwork.
