
// set the dimensions and margins of the graph
var margin_line = {top: 20, right: 20, bottom: 50, left: 70},
    width_line = 750 - margin_line.left - margin_line.right,
    height_line = 500 - margin_line.top - margin_line.bottom,
    annotation = {width: 100, height: 100, x:10, y: -30};

// formatting numbers
var bisectDate = d3.bisector(function(d){ return d.Year; }).left; // find y for a given x
var rounded = d3.format("d");
var formatNumber = d3.format("d");
//    format = function(d) { return formatNumber(d) + "m"; };
var year = 2021; // set initial year

// set the ranges
var x = d3.scaleLinear().range([0, width_line]);
var y = d3.scaleLinear().range([height_line, 0]);

// define the line
// area chart version
//var valueline = d3.area()
//    .x(function(d) { return x(d.Year); })
//    .y1(function(d) { return y(d.mean); })
//    .y0(y(0));
var valueline = d3.line()
    .x(function(d) { return x(d.Year); })
    .y(function(d) { return y(d.mean); });

// append the svg object to the body of the page
// appends a 'group' element to 'svg'
// moves the 'group' element to the top left margin
var svg_line = d3.select("#lake_mead").append("svg")
    .attr("width", width_line + margin_line.left + margin_line.right)
    .attr("height", height_line + margin_line.top + margin_line.bottom)
  .append("g")
    .attr("transform",
          "translate(" + margin_line.left + "," + margin_line.top + ")");

// Get the data
d3.csv("/koppen/map/lake_mead_metric.csv").then(function(data) {

//  // format the year column
//  data.forEach(function(d) {
//  d.Year = formatted(d.Year)
//  d.mean = +d.mean;
//  });

  // Scale the range of the data
  x.domain([d3.min(data, function(d) { return d.Year; }), d3.max(data, function(d) { return d.Year; })])
  .range([0, width_line]);
  y.domain(d3.extent(data, function(d) { return d.mean; }))
  .range([height_line, 0]);

  // Add the valueline path.
  svg_line.append("path")
      .data([data])
      .attr("class", "line")
      .attr("fill", "none")
      .attr("stroke", "#660066")
      .attr("stroke-width", 1.5)
      .attr("d", valueline);

  // Add the x Axis
  svg_line.append("g")
      .attr("transform", "translate(0," + height_line + ")")
      .call(d3.axisBottom(x)
      .tickFormat(d3.format('d'))); // this gets rid of the comma separator for thousands

  // Add the y Axis
  svg_line.append("g")
      .call(d3.axisLeft(y)
      .tickFormat(function(d) { return formatNumber(d) + "m"; }));

  // Add X axis label:
//  svg_line.append("text")
//    .attr("text-anchor", "end")
//    .attr("x", width_line)
//    .attr("y", height_line + margin_line.top + 20)
//    .text("X axis title");

  // Y axis label:
  svg_line.append("text")
    .attr("text-anchor", "end")
    .attr("transform", "rotate(-90)")
    .attr("y", -margin_line.left+20)
    .attr("x", -margin_line.top)
    .attr("style","font-family: 'Open Sans';font-weight: 400;")
    .text("Depth at Hoover Dam");

  //annotation
  var focus = svg_line.append("g")
            .attr("class", "focus");

    // outer circle
    focus.append("circle")
        .attr("r", 10)
        .attr("fill", "none")
        .attr("stroke","#bb0000")
        .attr("stroke-width",1)
        .attr("class", "outer_circle");

    // inner dot
    focus.append("circle")
        .attr("r", 4)
        .attr("fill", "#ffd140")
        .attr("stroke", "#bb0000")
        .attr("class", "inner_circle");

    // tooltip box
    focus.append("rect")
        .attr("class", "tooltip")
        .attr("width", 50)
        .attr("height", 50)
//        .attr("x", 15)
        .attr("x", -65)
        .attr("y", -22)
        .attr("rx", 4)
        .attr("ry", 4);

    // x label
        focus.append("text")
        .attr("class", "tooltip-year")
//        .attr("x", 22)
        .attr("x",-60)
        .attr("y", -2);

    // y data
        focus.append("text")
        .attr("class", "tooltip-depth")
//        .attr("x", 22)
        .attr("x",-60)
        .attr("y", 18)
        .attr("color: #BB00000");

    // y unit label
        focus.append("text")
//        .attr("x", 51)
        .attr("x",-32)
        .attr("y", 18)
        .text("m");


        svg_line.append("rect")
        .attr("class", "overlay")
        .attr("width", width_line)
        .attr("height", height_line);

    // set starting highlight
        var x0 = year,
                i = bisectDate(data, x0 + 1, 1) ,
                d0 = data[i - 1],
                d1 = data[i],
                d = x0 - d0.Year > d1.Year - x0 ? d1 : d0;
            focus.attr("transform", "translate(" + x(d.Year) + "," + y(d.mean) + ")");
            focus.select(".tooltip-year").text(d.Year);
            focus.select(".tooltip-depth").text(rounded(d.mean));

    // update the highlight
        function update(selectedYear) {
            var x0 = selectedYear,
                i = bisectDate(data, x0 + 1, 1) ,
                d0 = data[i - 1],
                d1 = data[i],
                d = x0 - d0.Year > d1.Year - x0 ? d1 : d0;
            focus.attr("transform", "translate(" + x(d.Year) + "," + y(d.mean) + ")");
            focus.select(".tooltip-year").text(d.Year);
            focus.select(".tooltip-depth").text(rounded(d.mean));
        }

    // event listener for year slider
         d3.select("#year").on("change", function(d) {
           // recover the option that has been chosen
           var selectedYear = d3.select(this).property("value")
           // run the updateChart function with this selected option
           update(selectedYear);
       });

});