(() => {
  function byId(id) {
    return document.getElementById(id);
  }

  async function clearFailed() {
    const button = byId("clear-failed-button");
    const age = byId("clear-failed-age");
    const token = byId("clear-failed-token");
    const result = byId("clear-failed-result");
    if (!button || !age || !token || !result) {
      return;
    }
    const value = age.value.trim() || "7d";
    const secret = token.value.trim();
    if (!secret) {
      result.textContent = "token required";
      return;
    }
    button.disabled = true;
    result.textContent = "clearing...";
    try {
      // A 401 here is deliberately NOT routed through the session-expiry
      // handler below: this call authenticates with the token typed into
      // the box next to it, so its 401 usually means "wrong token", and
      // throwing the page away to ask for a password would be the wrong
      // answer to a typo. It also already fails visibly -- the message
      // lands in `result` -- which is the property the polling fragments
      // lacked.
      const response = await fetch(
        `/api/v1/queue/clear-failed?older_than=${encodeURIComponent(value)}`,
        {
          method: "POST",
          headers: { Authorization: `Bearer ${secret}` },
        },
      );
      const text = await response.text();
      result.textContent = response.ok ? text : `failed: ${text}`;
      if (response.ok && window.htmx) {
        window.htmx.trigger("#queue-table", "refresh");
      }
    } catch (error) {
      result.textContent = `failed: ${error}`;
    } finally {
      button.disabled = false;
    }
  }

  /* ---------------------------------------------------------------
     Session expiry.

     WHY THIS EXISTS: the fleet grid and the jobs table refresh
     themselves with hx-trigger="every 10s, refresh", and the session
     cookie behind those requests lives 12 h (authn.SESSION_TTL_SECONDS).
     When it expires, every one of those fragment requests answers 401 --
     and htmx 2.x does not swap a non-2xx response. The last good
     fragment therefore stays on screen, the poll keeps firing, the
     relative timestamps keep ticking, and the console looks completely
     alive while showing data that is arbitrarily old. Nothing in the UI
     said otherwise; dashboard.js had no 401 handling at all.

     That is the same failure class as a stale snapshot, and it gets the
     same treatment the rest of this console gives it: say so out loud,
     then offer the way back.
     --------------------------------------------------------------- */

  const LOGIN_PATH = "/fleet/login";

  /* Notice first, redirect after -- rather than redirecting on the spot.

     A bare redirect answers "you need to log in". It does not answer the
     question that actually matters, which is "how long have I been
     reading a frozen page?". An operator who is silently bounced to a
     login form, logs back in and lands on a fresh grid never learns that
     the numbers they were quoting for the last hour were stale, so the
     one fact worth surfacing is the one a bare redirect throws away. A
     page that navigates by itself, with nothing clicked, also reads as a
     bug or a dropped connection, and it eats whatever the operator had
     in flight (a half-typed filter) without explanation.

     The other extreme -- notice only, no redirect -- is worse: a banner
     that has to be clicked is a banner that gets ignored, and the page
     goes on polling behind it. So: pin the notice, then navigate. The
     notice carries its own "Log in now" link, which makes the timer a
     convenience rather than the only route back.

     Four seconds is long enough to read one sentence and short enough
     that nobody starts wondering whether the page has hung. */
  const NOTICE_MS = 4000;

  // One-shot. Several fragments poll independently, so an expired
  // session arrives as a burst of 401s, not one -- without this the
  // banner would stack and the navigation would be scheduled repeatedly.
  let sessionExpiryHandled = false;

  function loginUrl() {
    const here = window.location.pathname + window.location.search;
    return `${LOGIN_PATH}?next=${encodeURIComponent(here)}`;
  }

  function showExpiredNotice() {
    const notice = document.createElement("div");
    notice.className = "banner banner-bad";
    notice.setAttribute("role", "alert");
    notice.id = "session-expired-notice";
    // Pinned, not inserted at the top of <main> where the other banners
    // live. An operator reading the bottom of a long jobs table would
    // never see an off-screen banner, and "the warning was there, just
    // not where you were looking" is the very failure being fixed here.
    // Positioning only -- the colours stay the stylesheet's.
    notice.style.cssText =
      "position:fixed;top:0;left:0;right:0;z-index:1000;" +
      "margin:0;border-radius:0;";

    const title = document.createElement("strong");
    title.textContent = "Your session expired and this page stopped updating.";
    const body = document.createElement("span");
    body.textContent =
      "Everything above is as old as the moment your session ran out. " +
      "Sending you to the login form… ";
    const link = document.createElement("a");
    // Set as a property, never interpolated into markup: the path and
    // query it encodes come from the address bar.
    link.href = loginUrl();
    link.textContent = "Log in now";

    notice.append(title, body, link);
    document.body.prepend(notice);
  }

  function handleUnauthorized() {
    // Redirect-loop guard: the login page is where a 401 sends people,
    // so a 401 raised *on* it must not send them there again.
    if (window.location.pathname.startsWith(LOGIN_PATH)) {
      return;
    }
    if (sessionExpiryHandled) {
      return;
    }
    sessionExpiryHandled = true;
    showExpiredNotice();
    window.setTimeout(() => {
      window.location.assign(loginUrl());
    }, NOTICE_MS);
  }

  function statusOf(event) {
    const xhr = event.detail && event.detail.xhr;
    return xhr ? xhr.status : 0;
  }

  document.addEventListener("DOMContentLoaded", () => {
    const button = byId("clear-failed-button");
    if (button) {
      button.addEventListener("click", clearFailed);
    }

    // Both events, on purpose.
    //
    // htmx:responseError is the one that fires today, because htmx 2.x
    // classifies a 401 as an error and refuses the swap. htmx:beforeSwap
    // fires for *every* response and is where that refusal can be
    // overridden -- htmx.config.responseHandling is the documented way to
    // make 4xx swap. If anyone ever turns that on, responseError stops
    // firing for 401 and a login page (or a JSON error body) would get
    // painted into the grid instead. Pinning shouldSwap = false here
    // keeps that from becoming a new way to lie to the operator.
    //
    // 401 only, never 403: a 403 means the session is fine and the role
    // is not (an operator opening /fleet/audit), and bouncing that to
    // the login form just re-issues the same credentials in a loop.
    document.body.addEventListener("htmx:beforeSwap", (event) => {
      if (statusOf(event) === 401) {
        event.detail.shouldSwap = false;
        handleUnauthorized();
      }
    });
    document.body.addEventListener("htmx:responseError", (event) => {
      if (statusOf(event) === 401) {
        handleUnauthorized();
      }
    });
  });
})();
