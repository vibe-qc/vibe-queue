# vq release artwork — house style and paste-ready prompts

Canvas **1672 x 941 px** (16:9) PNG, same as every image in the vibe-qc
series. Ask for 16:9 landscape at that canvas. If the generator returns a
different size, resize the full result to 1672 x 941. Do not crop to get there, the compositions are built for the full
frame.

## Why these look different from vibe-qc's

vibe-qc's constant is a **photoreal animal on a molecular lattice**, because
its codenames are chemists and physicists paired with animals. vq's codenames
are computer scientists paired with **the concept they are known for**, so
there is no animal to render and forcing one in would contradict the naming
scheme.

What makes this a series instead of twenty-seven unrelated pictures is a shared
substrate:

> **The queue.** A row of identical machined tokens -- matte-white ceramic or
> brushed aluminium slabs, rounded corners, roughly domino-proportioned --
> advancing along a slender rail that crosses the lower third of the frame.
> Each token carries one thin illuminated edge: bright for running, dim for
> pending, dark for done. The rail runs off both edges of the frame; the queue
> is never shown to end.

The concept named in the codename is then what *happens* to that row. Same
two lighting treatments as vibe-qc, so the two products read as one hand:

* **A. Light studio** -- near-white / pale-blue seamless background, soft even
  studio light, glossy white surface, gentle reflections, shallow depth of
  field. Accents in translucent **teal** and **violet**. Calm, clinical,
  product-shot.
* **B. Dark cinematic** -- deep navy-black, dramatic high contrast, glowing
  particle networks and fine filament light. Accents in electric **blue** and
  warm **amber**.

Pick one per image. Never blend them.

## The one rule that matters most

**Nothing may look like a diagram.** These are computing concepts, and the
default failure mode of every image generator asked for "a hash tree" or "a
transaction" is a flat infographic: boxes, arrows, glowing circuit boards,
floating code, terminal windows, holographic UI. Every image here must be a
**photographed physical object** under studio or cinematic light. If it could
appear in a machine-tool catalogue, it is right. If it could appear on a SaaS
landing page, it is wrong.

Every prompt therefore ends with the same negative list. Keep it.

> No text, no lettering, no numerals, no logos, no watermark, no user
> interface, no screens, no code, no circuit boards, no arrows, no flowchart,
> no infographic, no cartoon, no illustration, no stylisation.

**No animals**, with exactly one sanctioned exception, noted at v0.12.0 --
animals are vibe-qc's register.

## File naming, matching vibe-qc

    docs/_static/images-codenames/NN-vq-vX.Y.0-slug.png

e.g. `13-vq-v0.13.0-grays-log.png`. Record each final prompt in
`docs/_static/images-codenames/prompts.json` (same shape vibe-qc uses:
version, name, file, prompt, theme, status, generator, canvas).

One correction carried over from the Pople's Puffin brief: it says to leave
the left third empty because the docs page crops and overlays there. **It does
not.** `.codename-art img` is `display:block; max-width:100%` with a radius
and a shadow. Compose for the full frame.

---

# The prompts

Every existing name from v0.7.0 through v0.26.0 is **settled** as of 2026-09-09: v0.7.0 through v0.10.0 were
already in use, and the v0.11.0 / v0.12.0 resolutions and the v0.13.0 to
v0.25.0 backfill were approved by the maintainer. Slugs and filenames follow
from them, so nothing here moves under you.

## v0.7.0 "Hoare's Pipeline" — treatment A

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue
> seamless studio background, soft even lighting, glossy white surface,
> gentle reflections, shallow depth of field. A slender polished rail crosses
> the lower third of the frame from edge to edge, carrying a single-file row
> of identical matte-white ceramic tokens with rounded corners, each with one
> thin illuminated edge in translucent teal. The rail passes through a
> horizontal tube of frosted optical glass, and each token inside the tube is
> wrapped in its own faint teal envelope of light, isolated from its
> neighbours. Violet light picks out the tube's rims. Calm, clinical,
> scientific product-shot aesthetic. No text, no lettering, no numerals, no
> logos, no watermark, no user interface, no screens, no code, no circuit
> boards, no arrows, no flowchart, no infographic, no cartoon, no
> illustration, no stylisation.

## v0.8.0 "Dahl's Simula" — treatment A

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue
> seamless studio background, soft even lighting, glossy white surface,
> gentle reflections. A slender polished rail crosses the lower third
> carrying a row of identical matte-white ceramic tokens with thin teal
> illuminated edges. Standing upright across the rail, edge-on to the camera,
> is a tall pane of frosted glass with a violet glow along its edge. The
> tokens do not pass through it: the last token on the near side rests
> against the pane, and an identical token stands just beyond it on the far
> side, so the boundary is unmistakably real and something crossed it anyway.
> Calm, clinical, scientific product-shot aesthetic. No text, no lettering,
> no numerals, no logos, no watermark, no user interface, no screens, no
> code, no circuit boards, no arrows, no flowchart, no infographic, no
> cartoon, no illustration, no stylisation.

## v0.9.0 "Tukey's Window" — treatment A

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue
> seamless studio background, soft even lighting, glossy white surface. A
> long rail crosses the lower third carrying a row of many identical
> matte-white ceramic tokens. A precision-machined rectangular aperture
> frame, white anodised aluminium with a fine teal inner bevel, hovers just
> above the row. The four or five tokens framed by the aperture are crisply
> lit and razor sharp with bright teal edges; every token outside the
> aperture falls away into soft blur and dim violet shadow. The transition at
> the aperture's edges is smooth, tapered, not abrupt. Calm, clinical,
> scientific product-shot aesthetic. No text, no lettering, no numerals, no
> logos, no watermark, no user interface, no screens, no code, no circuit
> boards, no arrows, no flowchart, no infographic, no cartoon, no
> illustration, no stylisation.

## v0.10.0 "Lampson's Hint" — treatment A

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue
> seamless studio background, soft even lighting, glossy white surface,
> gentle reflections. Three slender parallel rails run left to right across
> the lower half, each carrying identical matte-white ceramic tokens with
> teal illuminated edges. Beside the middle rail stands a small teal pennant
> on a slim polished post -- clearly only a marker, nothing blocking the
> track, the rail beyond it completely clear and unobstructed. The tokens
> approaching it have visibly changed lanes onto the outer two rails, leaving
> the middle rail empty past the pennant. Calm, clinical, scientific
> product-shot aesthetic. No text, no lettering, no numerals, no logos, no
> watermark, no user interface, no screens, no code, no circuit boards, no
> arrows, no flowchart, no infographic, no cartoon, no illustration, no
> stylisation.

## v0.11.0 "Eager's Sharing" — treatment A

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue
> seamless studio background, soft even lighting, glossy white surface. A
> single slender rail enters from the left carrying identical matte-white
> ceramic tokens with teal illuminated edges, and divides through a smooth
> machined junction into four parallel rails of exactly equal length running
> to the right edge. Each of the four rails carries precisely the same number
> of tokens, evenly spaced, conspicuously balanced. Faint violet light traces
> the junction's dividing curves. Calm, clinical, scientific product-shot
> aesthetic. No text, no lettering, no numerals, no logos, no watermark, no
> user interface, no screens, no code, no circuit boards, no arrows, no
> flowchart, no infographic, no cartoon, no illustration, no stylisation.

## v0.12.0 "Hopper's Bug" — treatment B

**The one sanctioned animal in the series.** Grace Hopper's logged bug was a
literal moth, and it is the most legible artefact in computing history. It is
a deliberate one-off, not licence for animals elsewhere.

> Photorealistic 3-D render, 16:9 landscape, dark cinematic. Deep navy-black
> background, dramatic high contrast, a dark reflective floor. A slender rail
> crosses the lower third carrying a row of identical matte-white ceramic
> tokens, their edges glowing electric blue. One token near the centre has
> cleanly fractured. Resting on the fractured token is a single real moth,
> photoreal and anatomically correct, small, wings folded, softly lit. From
> the fracture a fine filament of warm amber light traces backward along the
> rail through several tokens to a single point that glows brightly, making
> the cause of the break traceable by eye. Cinematic, high contrast, no
> clutter. No text, no lettering, no numerals, no logos, no watermark, no
> user interface, no screens, no code, no circuit boards, no arrows, no
> flowchart, no infographic, no cartoon, no illustration, no stylisation.

## v0.13.0 "Gray's Log" — treatment A

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue
> seamless studio background, soft even lighting, glossy white surface. A
> slender rail crosses the lower third carrying a row of identical
> matte-white ceramic tokens with teal illuminated edges, moving to the
> right. Behind and slightly above the rail, a long continuous ribbon of
> frosted glass unspools from a polished reel and runs off the left edge of
> the frame. The ribbon carries a series of small raised teal marks, one per
> token that has already passed, evenly spaced along its whole visible
> length -- abstract impressions, tactile, never characters or symbols. The
> ribbon is unbroken and clearly cannot be rewound. Calm, clinical,
> scientific product-shot aesthetic. No text, no lettering, no numerals, no
> logos, no watermark, no user interface, no screens, no code, no circuit
> boards, no arrows, no flowchart, no infographic, no cartoon, no
> illustration, no stylisation.

## v0.14.0 "Needham's Principal" — treatment A

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue
> seamless studio background, soft even lighting, glossy white surface,
> gentle reflections. In the foreground, two seals of identical size and
> finish -- solid machined cylinders of brushed steel, each with a
> differently cut face -- stand side by side before a single recessed reader
> plate of white ceramic. The reader's fine ring illuminates bright teal for
> the left seal and remains completely dark for the right one, though the two
> seals are otherwise indistinguishable. Behind them a slender rail carries a
> row of matte-white ceramic tokens, softly out of focus. Calm, clinical,
> scientific product-shot aesthetic. No text, no lettering, no numerals, no
> logos, no watermark, no user interface, no screens, no code, no circuit
> boards, no arrows, no flowchart, no infographic, no cartoon, no
> illustration, no stylisation.

## v0.15.0 "Schroeder's Authority" — treatment A

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue
> seamless studio background, soft even lighting, glossy white surface. A
> slender rail crosses the lower third carrying identical matte-white ceramic
> tokens with teal illuminated edges. The rail is interrupted by a precisely
> machined gate of white anodised aluminium, closed, with tokens halted and
> stacked behind it. A single slender polished rod runs from the gate away to
> the right and up to a small sealed cylinder of brushed steel resting on its
> own plinth, glowing faint violet -- visibly the thing holding the gate
> shut. The linkage between gate and cylinder is unbroken and easy to follow
> by eye. Calm, clinical, scientific product-shot aesthetic. No text, no
> lettering, no numerals, no logos, no watermark, no user interface, no
> screens, no code, no circuit boards, no arrows, no flowchart, no
> infographic, no cartoon, no illustration, no stylisation.

## v0.16.0 "Sutherland's Sketchpad" — treatment A

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue
> seamless studio background, soft even lighting, glossy white surface. Four
> parallel slender rails run left to right across the lower half, each
> carrying identical matte-white ceramic tokens with teal illuminated edges.
> Suspended above them at a shallow angle is a large sheet of optically clear
> glass. Etched into the glass in fine teal line-work are simplified contour
> outlines of the tokens below, positioned to match them exactly, one contour
> per real token. A single contour is picked out in bright violet and joined
> to its corresponding physical token by one taut, hair-fine violet filament
> running down through the air. Calm, clinical, scientific product-shot
> aesthetic. No text, no lettering, no numerals, no logos, no watermark, no
> user interface, no screens, no code, no circuit boards, no arrows, no
> flowchart, no infographic, no cartoon, no illustration, no stylisation.

## v0.17.0 "Corbató's Password" — treatment B

Revision constraint: all key profiles must be neutral mechanical shapes.
No crosses, cruciform profiles, plus signs or religious symbols. The final
image replaces the amber-lit cross-shaped profile with a notched key blade;
the exact edit prompt is recorded in the manifest.

> Photorealistic 3-D render, 16:9 landscape, dark cinematic. Deep navy-black
> background, dramatic high contrast, dark reflective floor. In the
> foreground a row of eight short cylinders of brushed steel stand upright,
> identical in size, each capped with a differently cut key profile in
> polished metal. Exactly one cylinder glows warm amber from within, its
> light spilling across the reflective floor; the rest are cold and unlit.
> Behind them, receding into darkness, a wall of dim identical recessed
> slots, most empty. Faint electric-blue rim light along the cylinders'
> edges. Cinematic, high contrast, uncluttered. No text, no lettering, no
> numerals, no logos, no watermark, no user interface, no screens, no code,
> no circuit boards, no arrows, no flowchart, no infographic, no cartoon, no
> illustration, no stylisation.

## v0.18.0 "Abadi's Delegation" — treatment A

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue
> seamless studio background, soft even lighting, glossy white surface,
> gentle reflections. A chain of four seals of decreasing size runs from left
> to right across the frame, each a machined cylinder of brushed steel
> standing on the white surface. Each seal's face is pressed against the top
> of the next smaller one, so an impression is visibly being handed along the
> line. The final and smallest seal rests on a matte-white ceramic token
> sitting on a slender rail, having just stamped it; that token's edge glows
> teal while the tokens ahead of it on the rail remain unlit. A soft violet
> glow marks each point of contact. Calm, clinical, scientific product-shot
> aesthetic. No text, no lettering, no numerals, no logos, no watermark, no
> user interface, no screens, no code, no circuit boards, no arrows, no
> flowchart, no infographic, no cartoon, no illustration, no stylisation.

## v0.19.0 "Merkle's Hash" — treatment A

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue
> seamless studio background, soft even lighting, glossy white surface. A
> structure of small polished spheres rises above a slender rail: eight
> silver-white spheres in a row at the base, pairing upward through four,
> then two, to a single sphere at the apex, joined by taut hair-fine teal
> filaments under visible tension. One base sphere has been lifted very
> slightly out of its seat, and the entire path of filaments and spheres from
> that sphere to the apex glows bright violet while every other path stays
> cool teal. Beneath the structure the rail carries matte-white ceramic
> tokens, softly out of focus. Calm, clinical, scientific product-shot
> aesthetic. No text, no lettering, no numerals, no logos, no watermark, no
> user interface, no screens, no code, no circuit boards, no arrows, no
> flowchart, no infographic, no cartoon, no illustration, no stylisation.

## v0.20.0 "Herlihy's Wait-Free" — treatment A

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue
> seamless studio background, soft even lighting, glossy white surface. Two
> slender rails cross each other at a shallow angle near the centre of the
> frame, each carrying a steady row of identical matte-white ceramic tokens
> with teal illuminated edges. At the crossing point there is no gate, no
> signal post, no barrier and no mechanism of any kind -- just clean empty
> polished surface -- and the two streams of tokens interleave smoothly
> through the intersection with even spacing, none of them stopped or
> bunched. Every token's edge is lit; not one is dim. The absence of
> machinery at the junction is the most noticeable thing in the frame. Calm,
> clinical, scientific product-shot aesthetic. No text, no lettering, no
> numerals, no logos, no watermark, no user interface, no screens, no code,
> no circuit boards, no arrows, no flowchart, no infographic, no cartoon, no
> illustration, no stylisation.

## v0.21.0 "Kilburn's Page" — treatment A

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue
> seamless studio background, soft even lighting, glossy white surface. A
> bank of twelve identical machined drawers in white anodised aluminium fills
> the frame in a three-by-four grid, each with the same plain polished pull
> and the same blank front plate. Five drawers are pulled out to visibly
> different depths. Each open drawer holds exactly one matte-white ceramic
> token on a fitted teal-lined tray, and every token is subtly distinct --
> different bevel, different surface finish, different edge glow intensity --
> though the drawer fronts are indistinguishable. Faint violet light in the
> depths of the open drawers. Calm, clinical, scientific product-shot
> aesthetic. No text, no lettering, no numerals, no logos, no watermark, no
> user interface, no screens, no code, no circuit boards, no arrows, no
> flowchart, no infographic, no cartoon, no illustration, no stylisation.

## v0.22.0 "Little's Law" — treatment A

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue
> seamless studio background, soft even lighting, glossy white surface,
> gentle reflections. A slender rail carrying identical matte-white ceramic
> tokens with teal illuminated edges passes across the face of a large
> precision gauge: a circular instrument in brushed steel and white enamel
> with a polished glass cover, a single slim teal needle, and a plain
> engraved arc with fine unlabelled tick marks and no numerals at all. The
> needle rests partway along the arc. A short violet index mark is set on the
> arc ahead of the needle. The gauge is clearly measuring the row of tokens
> crossing it. Calm, clinical, scientific product-shot aesthetic. No text, no
> lettering, no numerals, no logos, no watermark, no user interface, no
> screens, no code, no circuit boards, no arrows, no flowchart, no
> infographic, no cartoon, no illustration, no stylisation.

## v0.23.0 "Saltzer's Binding" — treatment A

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue
> seamless studio background, soft even lighting, glossy white surface. In
> the centre, two lengths of slender polished rail meet in a precision
> machined coupling, closed and seated, with bright teal light at the seam
> and matte-white ceramic tokens running continuously across the join. To
> either side, two further rail segments end in identical couplings that are
> open and unconnected, their faces clean and unlit, with no tokens on them.
> The connected join is the only lit thing in the frame. Shallow depth of
> field. Calm, clinical, scientific product-shot aesthetic. No text, no
> lettering, no numerals, no logos, no watermark, no user interface, no
> screens, no code, no circuit boards, no arrows, no flowchart, no
> infographic, no cartoon, no illustration, no stylisation.

## v0.24.0 "Cheney's Semispace" — treatment B

> Photorealistic 3-D render, 16:9 landscape, dark cinematic. Deep navy-black
> background, dramatic high contrast, dark reflective floor. Two identical
> rectangular bays sit side by side, each a recessed machined plate with a
> grid of fitted mounting points. The left bay holds an assembly of
> matte-white ceramic tokens, dimly lit in cold blue, with several mounting
> points already standing empty. The right bay holds a complete, brightly lit
> duplicate of that assembly, warm amber, fully seated. Between the two bays,
> three tokens hang in mid-air along a trail of fine amber light, caught
> being carried across. Cinematic, high contrast, uncluttered. No text, no
> lettering, no numerals, no logos, no watermark, no user interface, no
> screens, no code, no circuit boards, no arrows, no flowchart, no
> infographic, no cartoon, no illustration, no stylisation.

## v0.25.0 "Härder's Atomicity" — treatment B

> Photorealistic 3-D render, 16:9 landscape, dark cinematic. Deep navy-black
> background, dramatic high contrast, dark reflective floor. Two polished
> docking cradles face each other with a gap between them. A single
> matte-white ceramic token hangs precisely at the midpoint of the gap,
> suspended in a taut cage of hair-fine electric-blue filaments radiating to
> both cradles, held absolutely still and touching neither. Directly beneath
> it, recessed into the floor, a shallow well holds an exact mirror duplicate
> of the token, dimly lit in warm amber, fully seated and immobile. Nothing
> in the frame is half-placed or partially inserted. Cinematic, high
> contrast, uncluttered. No text, no lettering, no numerals, no logos, no
> watermark, no user interface, no screens, no code, no circuit boards, no
> arrows, no flowchart, no infographic, no cartoon, no illustration, no
> stylisation.

## v0.26.0 "Raymond's Bazaar" — treatment A

Cut 2026-09-09. The release where vq became publishable: the MPL text it had
been declaring, a security policy, a contributor guide, a changelog, a
documentation site, the codename catalogue, and a release procedure. The
concept is a closed thing opening — the queue leaving the enclosure it was
built inside.

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue
> seamless studio background, soft even lighting, glossy white surface,
> gentle reflections. A slender polished rail carrying identical matte-white
> ceramic tokens with teal illuminated edges runs from a single enclosed
> channel on the left out into open space on the right, where it divides into
> many short rails fanning outward at slightly different angles, each with its
> own tokens. The enclosure's end is open and unsealed, its frame catching a
> soft violet edge light. On the open side the surface is brighter and less
> shadowed than inside the channel. Calm, clinical, scientific product-shot
> aesthetic, generous empty space. No text, no lettering, no numerals, no
> logos, no watermark, no user interface, no screens, no code, no circuit
> boards, no arrows, no flowchart, no infographic, no cartoon, no
> illustration, no stylisation.

---

## If a render comes back wrong

Ranked by how often it happens:

1. **It made a diagram.** Add "photographed physical object, machine-tool
   catalogue photography, no graphic design" and regenerate. Do not try to
   fix a diagram by editing.
2. **It wrote text on something.** The gauge, the drawer fronts and the log
   ribbon attract lettering. Add "every surface completely blank and
   unmarked".
3. **The tokens are inconsistent between images.** Generate one image you
   like first, then attach it as a style reference for the rest. v0.9.0
   "Tukey's Window" is the best anchor: it shows the most tokens, lit and
   unlit, in the cleanest light.
4. **Too busy.** These compositions are mostly empty surface on purpose.
   Add "minimal, generous empty space, single clear subject".


## Forward codenames — provisional

The v0.27.0 through v0.33.0 names are provisional. Re-confirm each against
the delivered concept when its release is cut; these are not shipped features.

## v0.27.0 "Fidge's Timestamp" — treatment A

Provisional. Logical ordering without a shared timepiece. The repo already carries docs/v0_7_1_lamports_clock_design.md, so this is grounded rather than invented.

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue seamless studio background, soft even lighting, glossy white surface, gentle reflections. Two slender polished rails run left to right at different heights, each carrying identical matte-white ceramic tokens with teal illuminated edges, the two rows advancing at visibly different spacings. Every token carries a small brushed-brass detent ring set into its face, machined with fine radial steps, each ring rotated one step further than the token behind it. Where the two rails pass closest, one token from each row has its ring turned to exactly the same step, and both glow violet. There is no clock, dial, or timepiece anywhere in the frame. Calm, clinical, scientific product-shot aesthetic. No text, no lettering, no numerals, no logos, no watermark, no user interface, no screens, no code, no circuit boards, no arrows, no flowchart, no infographic, no cartoon, no illustration, no stylisation.

## v0.28.0 "Mattern's Cut" — treatment B

Provisional. A consistent cut across hosts that are never stopped together.

> Photorealistic 3-D render, 16:9 landscape, dark cinematic. Deep navy-black background, dramatic high contrast, dark reflective floor. Four slender rails run left to right at different heights, each carrying identical matte-white ceramic tokens with edges glowing electric blue. A single sheet of pale amber light stands vertically across all four rails, thin as a blade and perfectly flat. Every token behind the sheet is frozen mid-travel with a fine crust of frost on its leading face; every token ahead of it is clean and still moving, caught with faint motion blur. The sheet meets each rail at a different point along its length, yet reads unmistakably as one instant. Cinematic, high contrast, uncluttered. No text, no lettering, no numerals, no logos, no watermark, no user interface, no screens, no code, no circuit boards, no arrows, no flowchart, no infographic, no cartoon, no illustration, no stylisation.

## v0.29.0 "Braden's Requirements" — treatment A

Provisional. Liberal in what you accept, conservative in what you emit. The posture of a queue whose hosts misbehave.

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue seamless studio background, soft even lighting, glossy white surface. On the left, a wide flared intake funnel of white anodised aluminium receives a jumble of tokens of visibly different sizes, thicknesses, bevels and finishes, some chipped, some skewed, tumbling in loosely. On the right, a narrow precision-machined outlet emits a single-file row of perfectly identical matte-white ceramic tokens, evenly spaced, every teal illuminated edge the same brightness, running away along a slender rail. Violet light rims the intake; teal light rims the outlet. The contrast between the two mouths is the subject of the picture. Calm, clinical, scientific product-shot aesthetic. No text, no lettering, no numerals, no logos, no watermark, no user interface, no screens, no code, no circuit boards, no arrows, no flowchart, no infographic, no cartoon, no illustration, no stylisation.

## v0.30.0 "Bloom's Filter" — treatment A

Provisional. Cheap probabilistic membership; no false negatives. Fits dedup and idempotency keys.

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue seamless studio background, soft even lighting, glossy white surface. A slender rail carrying identical matte-white ceramic tokens with teal illuminated edges runs toward a thick upright plate of white ceramic pierced by a dense irregular field of small round holes. Most tokens strike the solid plate and are stopped cleanly, their edges gone dark. A few pass through holes and continue on the far side, still lit teal. One of those has visibly been let through a hole it only partly fits, and its edge is tinted violet rather than teal. The plate is uniform, blank and unmarked. Shallow depth of field. Calm, clinical, scientific product-shot aesthetic. No text, no lettering, no numerals, no logos, no watermark, no user interface, no screens, no code, no circuit boards, no arrows, no flowchart, no infographic, no cartoon, no illustration, no stylisation.

## v0.31.0 "Stonebraker's Vacuum" — treatment B

Provisional. Reclaiming space that is dead but still occupied. Fits archive and auto-cleanup.

> Photorealistic 3-D render, 16:9 landscape, dark cinematic. Deep navy-black background, dramatic high contrast, dark reflective floor. A slender rail crosses the lower third carrying identical matte-white ceramic tokens; most edges glow electric blue, but a scattered several are dull, grey and visibly spent. Beneath the rail a recessed machined trough runs its length, and the spent tokens are being drawn down into it along fine threads of warm amber light, leaving clean empty seats that the lit tokens have already closed up to fill. The trough's depths are dark and its contents indistinct. The rail above is unbroken and continuous. Cinematic, high contrast, uncluttered. No text, no lettering, no numerals, no logos, no watermark, no user interface, no screens, no code, no circuit boards, no arrows, no flowchart, no infographic, no cartoon, no illustration, no stylisation.

## v0.32.0 "Brewer's Partition" — treatment B

Provisional. The split you tolerate rather than prevent.

> Photorealistic 3-D render, 16:9 landscape, dark cinematic. Deep navy-black background, dramatic high contrast, dark reflective floor. A slender rail crosses the frame and is cleanly severed at the centre, the two cut faces separated by a narrow dark gap with nothing bridging it. Both halves continue to carry identical matte-white ceramic tokens with edges glowing electric blue, each side advancing under its own warm amber light source, neither side dark or halted. The two halves are lit from opposite directions, so their shadows fall opposite ways. The empty gap is the highest-contrast element in the frame. Cinematic, high contrast, uncluttered. No text, no lettering, no numerals, no logos, no watermark, no user interface, no screens, no code, no circuit boards, no arrows, no flowchart, no infographic, no cartoon, no illustration, no stylisation.

## v0.33.0 "Erlang's Blocking" — treatment A

Provisional. A counted permit. Fits admission control and concurrency caps.

> Photorealistic 3-D product render, 16:9 landscape. Near-white pale-blue seamless studio background, soft even lighting, glossy white surface. A slender rail crosses the lower third carrying identical matte-white ceramic tokens with teal illuminated edges. Midway along it, a machined white aluminium housing holds a rack of exactly three short brushed-steel permit pins standing upright in fitted sockets. Two sockets are empty, and those two pins are seated in the faces of the only two tokens occupying the stretch of rail beyond the housing. The third pin still stands in its socket. Behind the housing a queue of tokens waits with dark, unlit edges. A faint violet glow marks the two empty sockets. Calm, clinical, scientific product-shot aesthetic. No text, no lettering, no numerals, no logos, no watermark, no user interface, no screens, no code, no circuit boards, no arrows, no flowchart, no infographic, no cartoon, no illustration, no stylisation.

## Generation provenance

The approved v0.9.0 **Tukey's Window** image,
`docs/_static/images-codenames/9-vq-v0.9.0-tukeys-window.png`, is the style
reference for all 26 other images. Its rounded ceramic slabs and polished
rail define the shared physical design. Each other render uses that image
as a reference while retaining its own brief prompt and lighting treatment.
The manifest records the exact reference instruction and generation prompt.

v1.0.0 **Liskov's Substitution** is deferred. Before adding it, generalise
`test_the_series_is_contiguous_from_its_first_minor` to check contiguity
within each major line. The present catalogue remains contiguous at 0.7–0.33.

## Separate brand design task

The light/dark vector wordmarks, favicon, and 1200 × 630 Open Graph card
landed separately in `a0d3511` while the release artwork was being generated.
They live in `docs/_static/logo/` and are wired into `docs/conf.py` and
`docs/index.md`.
See that directory's README for the vector sources and regeneration workflow.
They were not made with the image generator and remain separate from this
release-artwork series.

All 27 final PNGs generated on 2026-09-09 are native 1672 × 941 outputs;
no resizing or cropping was needed. Four scenes received targeted refinement
or regeneration: Sketchpad, Hash, Clock and Snapshot. The exact requests are
in the manifest, including the discarded Snapshot attempt separately from
its final generation prompt. Final PNGs total 37.18 MiB (35.98 MiB new).
