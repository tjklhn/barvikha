// This self invoked function creates `window.uga` function
// And sets `window.uga` function as default `GoogleAnalyticsObject`
// The name `uga` is necessary, because @kleinanzeigen/google-analytics uses that name
// modified from source: https://developers.google.com/analytics/devguides/collection/analyticsjs
(function () {
  const gaFunctionName = "uga";

  function ugaFunction(...args) {
    (window[gaFunctionName].q = window[gaFunctionName].q || []).push(args);
  }
  try {
    window.GoogleAnalyticsObject = gaFunctionName;

    // window[gaFunctionName].q is referring to queue of Google Analytics library
    // and it is processed when GA library is loaded
    window[gaFunctionName] = window[gaFunctionName] || ugaFunction;
    window[gaFunctionName].l = 1 * new Date();
  } catch (err) {
    console.error(err, "Belen.GoogleAnalyticsQueueInitializer");
  }
})();
