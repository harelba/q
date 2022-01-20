// Monitor all download links in GA

var dlCnt = 0;
var tocCnt = 0;

function GAizeDownloadLink(a) {
        var url = a.href;
        var x = url.indexOf("?");
        if (x != -1) {
            url = url.substr(0, x);
        }
        var url_test = url.match(/^http.*(archive\/|releases\/)(?<path>.*)/);
        if (url_test) {
            a.event_action = url_test.groups.path;
            console.log("Converting download link to be GA aware: " + url + " . download path is " + a.event_action);
            dlCnt = dlCnt + 1;
            a.onclick = function() {
                console.log("Sending GA event for link" + url);
                var that = this;
                gtag('event','perform download', { 'event_category': 'Downloads', 'event_label': 'Download ' + this.event_action  , 'value': 1 });
                setTimeout(function() {
                    location.href = that.href;
                }, 500);
                return false;
            };
        }
}

function GAizeTOCLink(l) {
	tocCnt = tocCnt + 1;
           l.onclick = function() {
               url_test = l.href.match(/^https?:\/\/.+(#.*)$/i);
               toc_name = url_test[1];
                var that = this;
                console.log("Sending GA event for toc link " + this.href);
                
                gtag('event','navigate', { 'event_category': 'Navigation', 'event_label': 'go to ' + toc_name, 'value': 1 });
                setTimeout(function() {
                    location.href = that.href;
                }, 250);
                return false;
            };

}

window.onload = function() {
    var anchors = document.getElementsByTagName('a');
    for (i = 0; i < anchors.length; i++) {
      GAizeDownloadLink(anchors[i]);
    }
    var toc_links = document.querySelectorAll('div.md-sidebar[data-md-component=toc] a.md-nav__link');
    for (i = 0; i < toc_links.length; i++) {
      GAizeTOCLink(toc_links[i]);
    }
    console.log("Converted " + dlCnt + " download links and " + tocCnt + " TOC links to be GA aware");
}
