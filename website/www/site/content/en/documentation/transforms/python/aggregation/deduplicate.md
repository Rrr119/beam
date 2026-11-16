---
title: "Deduplicate"
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Deduplicate

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.deduplicate" class="DeduplicateWindowed" >}}

Removes duplicate elements over a bounded time duration while keeping the
pipeline streaming-friendly. Instead of buffering the entire input, the
transform records an "already seen" marker in per-key, per-window state and
clears it again once a processing-time or event-time timer fires.

## When to use which transform

* `DeduplicateWindowed` deduplicates arbitrary elements, using the element
  itself as the identity. It is window aware: duplicates are suppressed
  independently within every window the element belongs to.
* `DeduplicatePerWindow` does the same for keyed `(key, value)` collections
  and uses the key as the identity, keeping the first observed value.
* `Deduplicate` and `DeduplicatePerKey` are the earlier, non-window-aware
  variants. They keep their existing behavior for backwards compatibility;
  new pipelines that apply windowing should use the window-aware variants.

At least one of `processing_time_duration` or `event_time_duration` must be
provided; the marker state for a key is cleared when the first timer fires.

## Window and state backend compatibility

`DeduplicateWindowed` and `DeduplicatePerWindow` work with both non-merging
window functions (`FixedWindows`, `SlidingWindows`) and merging window
functions (`Sessions`):

* The seen marker is stored in mergeable bag state, so when session windows
  merge the marker is carried into the merged window and duplicates stay
  suppressed. Read-modify-write (value) state is not mergeable and therefore
  cannot be used with merging windows.
* For merging window functions an internal grouping step is applied before
  the stateful `ParDo`. Window merging is driven by grouping operations, so
  without this step elements in windows that should merge would keep
  independent state and duplicates would leak through.

This keeps the transform portable across runners and state backends that
support the Beam state and timer APIs.

## Examples

In the following example a stream of events is assigned session windows and
then deduplicated. Repeated events that fall into the same (possibly merged)
session window are emitted only once.

```python
deduplicated = (
    events
    | beam.WindowInto(window.Sessions(gap_size=10))
    | deduplicate.DeduplicateWindowed(processing_time_duration=600))
```

The same pattern works with sliding windows, where each element may belong to
several windows but is deduplicated within each one:

```python
deduplicated = (
    events
    | beam.WindowInto(window.SlidingWindows(window_size=30, period=10))
    | deduplicate.DeduplicateWindowed(event_time_duration=300))
```

## Related transforms

* [Distinct](/documentation/transforms/python/aggregation/distinct)
  guarantees uniqueness across the whole input without a time bound, which may
  require buffering more state.
