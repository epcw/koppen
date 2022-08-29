const urls = {
  // source: https://observablehq.com/@mbostock/u-s-stations-voronoi
  // source: https://github.com/topojson/us-atlas
  map: "/koppen/map/states-albers-10m.json",

  // source: https://gist.github.com/mbostock/7608400
//  stations:
//    "https://gist.githubusercontent.com/mbostock/7608400/raw/e5974d9bba45bc9ab272d98dd7427567aafd55bc/stations.csv",
  stations:
    // "/koppen/map/stations_pilot.csv",
    "/koppen/map/stations_15.csv",

  // source: https://gist.github.com/mbostock/7608400
//  edges:
//  "https://gist.githubusercontent.com/mbostock/7608400/raw/e5974d9bba45bc9ab272d98dd7427567aafd55bc/flights.csv"
    edges:
    // "/koppen/map/edges_pilot.csv"
    "/koppen/map/edges_15.csv"
};

const svg  = d3.select("svg");

const width  = parseInt(svg.attr("width"));
const height = parseInt(svg.attr("height"));
const hypotenuse = Math.sqrt(width * width + height * height);

// must be hard-coded to match our topojson projection
// source: https://github.com/topojson/us-atlas
const projection = d3.geoAlbers().scale(1280).translate([480, 300]);

const scales = {
  // used to scale station bubbles
  stations: d3.scaleSqrt()
    .range([4, 28]),

  // used to scale number of segments per line
  segments: d3.scaleLinear()
    .domain([0, hypotenuse])
    .range([1, 10])
};

// have these already created for easier drawing
const g = {
  basemap:  svg.select("g#basemap"),
  edges:  svg.select("g#edges"),
  stations: svg.select("g#stations"),
  voronoi:  svg.select("g#voronoi")
};

console.assert(g.basemap.size()  === 1);
console.assert(g.edges.size()  === 1);
console.assert(g.stations.size() === 1);
console.assert(g.voronoi.size()  === 1);

const tooltip = d3.select("text#tooltip");
console.assert(tooltip.size() === 1);
const tooltipdetail = d3.select("text#tooltipdetail");
console.assert(tooltipdetail.size() === 1);
const tooltipcode = d3.select("text#tooltipcode");
console.assert(tooltipcode.size() === 1);

//pzed create base year
//var minyear = d3.min(stations.year, function (d) { return d.val; });
//var maxyear = d3.max(stations.year, function (d) { return d.val; });
//var year = 1982;
//var years_averaged = 30;
var year = 1937;

// load and draw base map
d3.json(urls.map).then(drawMap);

// load the station and  data together
const promises = [
  d3.csv(urls.stations, typeStation),
  d3.csv(urls.edges,  typeEdge)
];

g.basemap.selectAll('path').remove();

d3.json(urls.map).then(drawMap);

Promise.all(promises).then(processData);

d3.select("#year").on("input", function () {
    year = this.value;
    d3.select('#year-value').text(year);
//    d3.select('#year').attr("min", minyear);
//    d3.select('#year').attr("max", maxyear);
    g.stations.selectAll('circle').remove();
    g.voronoi.selectAll('path').remove();

    Promise.all(promises).then(processData);


    // g.stations.selectAll('circle').each(function(d) {
    //     drawStations(g.stations);
    //   });
  });

//d3.select("#timeline").on("input", function () {
//    years_averaged = +this.value;
//    d3.select('#timeline-value').text(years_averaged);
////    d3.select('#year').attr("min", minyear);
////    d3.select('#year').attr("max", maxyear);
//    g.stations.selectAll('circle').remove();
//    g.voronoi.selectAll('path').remove();
//
//    Promise.all(promises).then(processData);
//  });

// process station and  data
function processData(values) {
  console.assert(values.length === 2);

  let stations = values[0];
  let edges  = values[1];

//  console.log("stations: " + stations.length);
//  console.log(" edges: " + edges.length);

  // convert stations array (pre filter) into map for fast lookup
  let noaa = new Map(stations.map(node => [node.noaa, node]));

  // calculate incoming and outgoing degree based on edges
  // edges are given by station noaa code (not index)
  edges.forEach(function(link) {
    link.source = noaa.get(link.origin);
    link.target = noaa.get(link.destination);

    link.source.outgoing = link.count;
    link.target.incoming += link.count;
  });

  // filter for year
  let oldyear = stations.year;
  stations = stations.filter(station => station.year == year );

  // remove stations out of bounds
  let old = stations.length;
  stations = stations.filter(station => station.x >= 0 && station.y >= 0);
//  console.log(" removed: " + (old - stations.length) + " stations out of bounds");

  // remove stations with NA state
  old = stations.length;
  stations = stations.filter(station => station.state !== "NA");
//  console.log(" removed: " + (old - stations.length) + " stations with NA state");

//  // remove stations without any edges
//  old = stations.length;
//  stations = stations.filter(zeroFilter) //pzed insert
//    stations = stations.filter(station => station.outgoing > 0 && station.incoming > 0);
//    stations = stations.filter(station => station.outgoing > 0);
//  console.log(" removed: " + (old - stations.length) + " stations without edges");

  // sort stations by outgoing degree
  stations.sort((a, b) => d3.descending(a.outgoing, b.outgoing));

  // keep only the top stations
//  old = stations.length;
//  stations = stations.slice(0, 500);
//  console.log(" removed: " + (old - stations.length) + " stations with low outgoing degree");

  // done filtering stations can draw
  drawStations(stations);
  drawPolygons(stations);

  // reset map to only include stations post-filter
  noaa = new Map(stations.map(node => [node.noaa, node]));

  // filter out edges that are not between stations we have leftover
  old = edges.length;
  edges = edges.filter(link => noaa.has(link.source.noaa) && noaa.has(link.target.noaa));
//  console.log(" removed: " + (old - edges.length) + " edges");

  // done filtering edges can draw
//  drawedges(stations, edges); //pzed commenting out until we figure out edges

//  console.log({stations: stations});
//  console.log({edges: edges});
}

//pzed create filter function for more than 0 outgoing
function zeroFilter(stations) {
    return stations.outgoing > 0;
}

// draws the underlying map
function drawMap(map) {
  // remove unwanted states
  map.objects.states.geometries = map.objects.states.geometries.filter(inArea);

  // run topojson on remaining states and adjust projection
  let land = topojson.merge(map, map.objects.states.geometries);

  // use null projection; data is already projected
  let path = d3.geoPath();

  // draw base map
  g.basemap.append("path")
    .datum(land)
    .attr("class", "land")
    .attr("d", path);

  // draw interior borders
  g.basemap.append("path")
    .datum(topojson.mesh(map, map.objects.states, (a, b) => a !== b))
    .attr("class", "border interior")
    .attr("d", path);

  // draw exterior borders
  g.basemap.append("path")
    .datum(topojson.mesh(map, map.objects.states, (a, b) => a === b))
    .attr("class", "border exterior")
    .attr("d", path);
}

function drawStations(stations) {
  // adjust scale
  const extent = d3.extent(stations, d => d.outgoing);
  scales.stations.domain(extent);
//  const koppen = station.koppen;

  // draw station bubbles
  g.stations.selectAll("circle.station")
    .data(stations, d => d.noaa)
    .enter()
    .append("circle")
    .attr("r", "4")
//    .attr("r",  d => scales.stations(d.outgoing) * -1/4 + 7 ) // pzed this changes the size of the circles.
    .attr("cx", d => d.x) // calculated on load
    .attr("cy", d => d.y) // calculated on load
//    .attr("class", "station ")
    .attr('class', function (station) { return "station " + station.koppen })
    .each(function(d) {
      // adds the circle object to our station
      // makes it fast to select stations on hover
      d.bubble = this;
    });
}

// ** Update data section (Called from the onclick) // PWZ kill this and don't try to do it on all one page.
//function updateStation15() {
//
//    // Get the data again
//    const urls = { stations: "/koppen/map/stations_15.csv", edges: "/koppen/map/edges_15.csv"};
//
//     var svg = d3.select("body").transition();
//    drawStations(stations);
//    drawedges(stations, edges);
//};

function drawPolygons(stations) {
  // convert array of stations into geojson format
  const geojson = stations.map(function(station) {
    return {
      type: "Feature",
      properties: station,
      geometry: {
        type: "Point",
        coordinates: [station.longitude, station.latitude]
      }
    };
  });

  // calculate voronoi polygons
  const polygons = d3.geoVoronoi().polygons(geojson);
//  console.log(polygons);

  g.voronoi.selectAll("path")
    .data(polygons.features)
    .enter()
    .append("path")
    .attr("d", d3.geoPath(projection))
    .attr("class", "voronoi")
    .on("mouseover", function(d) {
      let station = d.properties.site.properties;

      d3.select(station.bubble)
        .classed("highlight", true);

      d3.selectAll(station.edges)
        .classed("highlight", true)
        .raise();

      // make tooltip take up space but keep it invisible
      tooltip.style("display", null);
      tooltip.style("visibility", "hidden");
      tooltipdetail.style("display", null);
      tooltipdetail.style("visibility", "hidden");
      tooltipcode.style("display", null);
      tooltipcode.style("visibility", "hidden");

      // set default tooltip positioning
      tooltip.attr("text-anchor", "middle");
      tooltip.attr("dy", -scales.stations(station.outgoing) - 4);
      tooltip.attr("x", station.x);
      tooltip.attr("y", station.y);
      tooltipdetail.attr("text-anchor", "middle");
      tooltipdetail.attr("dy", -scales.stations(station.outgoing) + 10);
      tooltipdetail.attr("x", station.x);
      tooltipdetail.attr("y", station.y);
      tooltipcode.attr("text-anchor", "middle");
      tooltipcode.attr("dy", -scales.stations(station.outgoing) - 20);
      tooltipcode.attr("x", station.x);
      tooltipcode.attr("y", station.y);


      // set the tooltip text
      tooltipcode.text("Station: " + station.noaa);
      tooltip.text(station.name + " (" + station.elevation + "m)");
      tooltipdetail.text(station.year + " year average | " + station.koppen + " - " + station.koppen_name);

      // double check if the anchor needs to be changed
      let bbox = tooltip.node().getBBox();

      if (bbox.x <= 100) {
        tooltip.attr("text-anchor", "start");
        tooltipdetail.attr("text-anchor", "start");
        tooltipcode.attr("text-anchor", "start");
      }
      else if (bbox.x + bbox.width >= width + 140) {
        tooltip.attr("text-anchor", "end");
        tooltipdetail.attr("text-anchor", "end");
        tooltipcode.attr("text-anchor", "end");
      }

      if (bbox.y <= 50) {
        tooltip.attr("dy", -scales.stations(station.outgoing) + 25);
        tooltipdetail.attr("dy", -scales.stations(station.outgoing) + 40);
        tooltipcode.attr("dy", -scales.stations(station.outgoing) + 10);
      }

      tooltip.style("visibility", "visible");
      tooltipdetail.style("visibility", "visible");
      tooltipcode.style("visibility", "visible");
    })
    .on("mouseout", function(d) {
      let station = d.properties.site.properties;

      d3.select(station.bubble)
        .classed("highlight", false);

      d3.selectAll(station.edges)
        .classed("highlight", false);

      d3.select("text#tooltip").style("visibility", "hidden");
      d3.select("text#tooltipdetail").style("visibility", "hidden");
      d3.select("text#tooltipcode").style("visibility", "hidden");
    })
    .on("dblclick", function(d) {
      // toggle voronoi outline
      let toggle = d3.select(this).classed("highlight");
      d3.select(this).classed("highlight", !toggle);
    });
}
// pzed commenting out until we figure out edges
function drawedges(stations, edges) {
  // break each  between stations into multiple segments
  let bundle = generateSegments(stations, edges);

  // https://github.com/d3/d3-shape#curveBundle
  let line = d3.line()
    .curve(d3.curveBundle)
    .x(station => station.x)
    .y(station => station.y);

  let links = g.edges.selectAll("path.edge")
    .data(bundle.paths)
    .enter()
    .append("path")
/* failed attempt to add arrows
    .append('marker')
    .attr('id', 'arrow')
    .attr('viewBox', [0, 0, markerBoxWidth, markerBoxHeight])
    .attr('markerWidth', markerBoxWidth)
    .attr('markerHeight', markerBoxHeight)
    .attr('orient', 'auto-start-reverse')
    .attr('d', d3.line()(arrowPoints))
*/
    .attr('stroke', 'black')
    .attr("d", line)
    .attr("class", "edge")
    .each(function(d) {
      // adds the path object to our source station
      // makes it fast to select outgoing paths
      d[0].edges.push(this);
    });

  // https://github.com/d3/d3-force
  let layout = d3.forceSimulation()
    // settle at a layout faster
    .alphaDecay(0.1)
    // nearby nodes attract each other
    .force("charge", d3.forceManyBody()
      .strength(10)
      .distanceMax(scales.stations.range()[1] * 2)
    )
    // edges want to be as short as possible
    // prevents too much stretching
    .force("link", d3.forceLink()
      .strength(0.7)
      .distance(0)
    )
    .on("tick", function(d) {
      links.attr("d", line);
    })
    .on("end", function(d) {
//      console.log("layout complete");
    });

  layout.nodes(bundle.nodes).force("link").links(bundle.links);
}

// Turns a single edge into several segments that can
// be used for simple edge bundling.
function generateSegments(nodes, links) {
  // generate separate graph for edge bundling
  // nodes: all nodes including control nodes
  // links: all individual segments (source to target)
  // paths: all segments combined into single path for drawing
  let bundle = {nodes: [], links: [], paths: []};

  // make existing nodes fixed
  bundle.nodes = nodes.map(function(d, i) {
    d.fx = d.x;
    d.fy = d.y;
    return d;
  });

  links.forEach(function(d, i) {
    // calculate the distance between the source and target
    let length = distance(d.source, d.target);

    // calculate total number of inner nodes for this link
    let total = Math.round(scales.segments(length));

    // create scales from source to target
    let xscale = d3.scaleLinear()
      .domain([0, total + 1]) // source, inner nodes, target
      .range([d.source.x, d.target.x]);

    let yscale = d3.scaleLinear()
      .domain([0, total + 1])
      .range([d.source.y, d.target.y]);

    // initialize source node
    let source = d.source;
    let target = null;

    // add all points to local path
    let local = [source];

    for (let j = 1; j <= total; j++) {
      // calculate target node
      target = {
        x: xscale(j),
        y: yscale(j)
      };

      local.push(target);
      bundle.nodes.push(target);

      bundle.links.push({
        source: source,
        target: target
      });

      source = target;
    }

    local.push(d.target);

    // add last link to target node
    bundle.links.push({
      source: target,
      target: d.target
    });

    bundle.paths.push(local);
  });

  return bundle;
}

// determines which states belong to the set
// https://gist.github.com/mbostock/4090846#file-us-state-names-tsv
function inArea (state) {
  const id = parseInt(state.id);
  return id == 06 || id == 41 || id == 53 || id == 04 || id == 08 || id == 16 || id == 30 || id == 32 || id == 35 || id == 49 || id == 56; // Continental US West
//  return id < 60 && id !== 2 && id !== 15; //all continental US
}

// see stations.csv
// convert gps coordinates to number and init degree
function typeStation(station) {
  station.longitude = parseFloat(station.longitude);
  station.latitude  = parseFloat(station.latitude);

  // use projection hard-coded to match topojson data
  const coords = projection([station.longitude, station.latitude]);
  station.x = coords[0];
  station.y = coords[1];

  station.outgoing = 0;  // eventually tracks number of outgoing edges
  station.incoming = 0;  // eventually tracks number of incoming edges

  station.edges = [];  // eventually tracks outgoing edges

  return station;
}

// see edges.csv
// convert count to number
function typeEdge(edge) {
  edge.count = parseInt(edge.count);
  return edge;
}

// calculates the distance between two nodes
// sqrt( (x2 - x1)^2 + (y2 - y1)^2 )
function distance(source, target) {
  const dx2 = Math.pow(target.x - source.x, 2);
  const dy2 = Math.pow(target.y - source.y, 2);

  return Math.sqrt(dx2 + dy2);
}

// trying a zoom function here
var zoom = d3.zoom()
      .scaleExtent([1, 8])
      .on('zoom', function() {
         //transform = d3.event.transform //store transform
          g.stations.selectAll('circle').attr('transform', d3.event.transform),
          g.basemap.selectAll('path').attr('transform', d3.event.transform),
          g.voronoi.selectAll("path").attr('transform', d3.event.transform);
});

// svg.call(zoom); //kill until we get this working because it's ANNOYING