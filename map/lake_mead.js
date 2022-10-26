
// set the dimensions and margins of the graph
var margin_line = {top: 20, right: 20, bottom: 50, left: 70},
    width_line = 750 - margin_line.left - margin_line.right,
    height_line = 500 - margin_line.top - margin_line.bottom;

// parse the date / time
var parseTime = d3.timeParse("%d-%b-%y");

// set the ranges
var x = d3.scaleTime().range([0, width_line]);
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

// append the svg obgect to the body of the page
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

//  // format the data
//  data.forEach(function(d) {
//      d.Year = parseTime(d.Year);
//      d.mean = +d.mean;
//  });

  // Scale the range of the data
  x.domain([1950, d3.max(data, function(d) { return d.Year; })])
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
      .call(d3.axisBottom(x));

  // Add the y Axis
  svg_line.append("g")
      .call(d3.axisLeft(y));

});