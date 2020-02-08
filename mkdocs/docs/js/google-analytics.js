// Monitor all download links in GA
window.onload = function() {
    var a = document.getElementsByTagName('a');
    var cnt = 0;
    for (i = 0; i < a.length; i++) {
        var url = a[i].href;
        var x = url.indexOf("?");
        if (x != -1) {
            url = url.substr(0, x);
        }
        var url_test = url.match(/^https?:\/\/.+(\/rpms\/.*\.rpm|\/deb\/.*\.deb|single-binary\/Darwin\/.*\/q|\/archive\/.*\.tar\.gz|\/archive\/.*\.zip|\/windows\/.*\.exe)$/i);
        if (url_test) {
            console.log("Converting url to be GA aware: " + url);
            if (url_test.length > 1) {
                var event_action = url_test[1];
            } else {
                var event_action = 'unknown_action';
            }
            a[i].event_action = event_action;
            cnt = cnt + 1;
            a[i].onclick = function() {
                console.log("Sending GA event for link" + url);
                var that = this;
              //ga('send', 'event', 'Downloads', 'Click on ' + this.event_action, this.getAttribute('href'));
                gtag('event','perform download', { 'event_category': 'Downloads', 'event_label': 'Download ' + this.event_action  , 'value': 1 });
                setTimeout(function() {
                    location.href = that.href;
                }, 500);
                return false;
            };
        }
    }
    console.log("Converted " + cnt + " links to be GA aware");
}
