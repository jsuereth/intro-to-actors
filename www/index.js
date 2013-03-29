$(function() {
	
	function query(q) {
		return $.ajax({
			url: '/query',
			dataType: 'json',
			data: {
				q: q
			}
		});
	}
	
	function makeHtml(hotels) {
      if(hotels.length) {
    	  var buf = [];
    	  buf.push('<ul>');
    	  for(var i=0; i < hotels.length; ++i) {
    		  var hotel = hotels[i];
    		  buf.push('<li>');
    		  buf.push('<h2>');
    		  buf.push(hotel.name);
    		  buf.push('</h2>');
    		  buf.push('<p>');
    		  buf.push(hotel.description);
    		  buf.push('</p>');
    		  buf.push('</li>');
    	  }
    	  buf.push('</ul>');
    	  return buf.join('');
      }
      return 'No Results Found';
	}
	
	function render(data) {
      if(data.success == 'true')
	    $('#hotelList').html(makeHtml(data.results));
      else
    	$('#hotelList').html('Query failed!')
	}
	
	$('#searchButton').on('click', function() {
		var q = $('#searchBox').val();
		// TODO - Get unfiltered ot return error on any query failure.
		query(q).success(function(data) {
			render(data);
		}).error(function(e) {
			render({ success: false })
		});
	});
});