(function () {
  "use strict";

  function initialize(status) {
    if (!status || !status.isConnected || status.closest(".jenkins-hidden") ||
        status.dataset.rabbitmqPollingStarted === "true") {
      return;
    }
    // Named Stapler bindings may be global lexical variables, not window properties.
    var descriptor = typeof rabbitmqConsumerCspDescriptor === "undefined" ?
      null : rabbitmqConsumerCspDescriptor;
    if (!descriptor || typeof descriptor.isOpen !== "function") {
      return;
    }
    // Scope the image to this status row: hidden templates can duplicate IDs.
    function statusImage() {
      return status.parentNode && status.parentNode.querySelector("#rabbitmq-status-img");
    }
    status.dataset.rabbitmqPollingStarted = "true";

    function checkConnection() {
      var image = statusImage();
      if (!status.isConnected || !image || !image.isConnected) {
        return;
      }
      descriptor.isOpen(function (response) {
        var image = statusImage();
        if (!status.isConnected || !image || !image.isConnected || !response ||
            typeof response.responseObject !== "function") {
          return;
        }
        var connected = response.responseObject() == true;
        status.textContent = connected ? status.dataset.connected : status.dataset.disconnected;
        image.src = connected ? status.dataset.connectedIcon : status.dataset.disconnectedIcon;
      });
    }

    checkConnection();
    window.setInterval(checkConnection, 15000);
  }

  if (typeof Behaviour !== "undefined") {
    Behaviour.specify("#rabbitmq-status-text", "rabbitmq-consumer-csp", 0, initialize);
  } else if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      document.querySelectorAll("#rabbitmq-status-text").forEach(initialize);
    });
  } else {
    document.querySelectorAll("#rabbitmq-status-text").forEach(initialize);
  }
})();
