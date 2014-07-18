  (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
  (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
  m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
  })(window,document,'script','//www.google-analytics.com/analytics.js','ga');

  ga('create', 'UA-48316355-1', 'auto');
  ga('require', 'displayfeatures');
  ga('require', 'linkid', 'linkid.js');
  ga('send', 'pageview');

// Monitor all download links in GA
window.onload = function() {
  var a = document.getElementsByTagName('a');
  for (i = 0; i < a.length; i++) {
      if (a[i].href.match(/^https?:\/\/.+(\.rpm|\/q|\.tar\.gz|\.zip)$/i)) {
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
}

