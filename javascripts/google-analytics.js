  (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
  (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
  m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
  })(window,document,'script','//www.google-analytics.com/analytics.js','ga');

  ga('create', 'UA-48316355-1', 'auto');
  ga('require', 'displayfeatures');
  //ga('require', 'linkid', 'linkid.js');
  ga('send', 'pageview');

// Monitor all download links in GA
window.onload = function() {
  var a = document.getElementsByTagName('a');
  var cnt = 0;
  for (i = 0; i < a.length; i++) {
      var url = a[i].href;
      var x = url.indexOf("?");
      if (x != -1) {
        url = url.substr(0,x);
      }
      if (url.match(/^https?:\/\/.+(\.rpm|\.deb|\/q|\.tar\.gz|\.zip|\.bat|\.exe|python-api)$/i)) {
        cnt = cnt + 1;
        a[i].onclick = function() {
		  var that = this;
          ga('send', 'event', 'Downloads', 'Click', this.getAttribute('href'));
		  setTimeout(function() {
			location.href = that.href;
		  },500); 
		  return false;
        };
      }
  }
  console.log("Converted " + cnt + " links to be GA aware");
}

