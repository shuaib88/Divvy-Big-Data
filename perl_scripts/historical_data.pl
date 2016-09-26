#!/usr/bin/perl -w
# Creates an html table of Divvy ride info and weather for the date

use strict;
use warnings;
use 5.10.0;
use HBase::JSONRest;
use CGI qw/:standard/;

# Read the date as CGI parameters
my $date = param('date');

# Define a connection template to access the HBase REST server
# If you are on out cluster, hadoop-m will resolve to our Hadoop master
# node, which is running the HBase REST server
my $hbase = HBase::JSONRest->new(host => "hadoop-m:2056");

# This function takes a row and gives you the value of the given column
# E.g., cellValue($row, 'delay:rain_delay') gives the value of the
# rain_delay column in the delay family.
sub cellValue {
    my $row = $_[0];
    my $field_name = $_[1];
    my $row_cells = ${$row}{'columns'};
    foreach my $cell (@$row_cells) {
	if ($$cell{'name'} eq $field_name) {
	    return $$cell{'value'};
	}
    }
    return 'missing';
}

# Query both hbase tables using the date
my $records_north_table = $hbase->get({
  table => 'ahmed_northside_trip_and_weather',
  where => {
    key_equals => $date
  },
});
my $records_south_table = $hbase->get({
  table => 'ahmed_southside_trip_and_weather',
  where => {
    key_equals => $date
  },
});


# There will only be one record for this route, which will be the
# "zeroth" row returned
my $row_north = @$records_north_table[0];

my $row_south = @$records_south_table[0];
# Convert seconds to minutes:seconds 
# Source: http://neilang.com/articles/converting-seconds-into-a-readable-format-in-perl/
sub convert_time {
  my $time = shift;
  my $days = int($time / 86400);
  $time -= ($days * 86400);
  my $hours = int($time / 3600);
  $time -= ($hours * 3600);
  my $minutes = int($time / 60);
  my $seconds = $time % 60;

  $days = $days < 1 ? '' : $days .' days ';
  $hours = $hours < 1 ? '' : $hours .' hours ';
  $minutes = $minutes < 1 ? '' : $minutes . ' minutes ';
  $time = $days . $hours . $minutes . $seconds . ' seconds ';
  return $time;
}


sub convert_c_to_f {
  my $temp_c = shift;
  my $temp_f = int(($temp_c / 10) * (9/5) + 32);
  return sprintf($temp_f . ' F');
}

sub convert_mm_to_in {
  my $precip = shift;
  return sprintf("%.1f", $precip * 0.0397 * 0.1);
}

#north side variables
my($average_ride_time_north, $ride_count_north, $tmax_north, $tmin_north, $prcp_north, $snow_north) = 
  (cellValue($row_north, 'column:avg_trip_length'),
  cellValue($row_north, 'column:trip_count'),
  cellValue($row_north, 'column:tmax'),
  cellValue($row_north, 'column:tmin'),
  cellValue($row_north, 'column:prcp'),
  cellValue($row_north, 'column:snow'));

#south side variables _ INCOMPLETE
my($average_ride_time_south, $ride_count_south, $tmax_south, $tmin_south, $prcp_south, $snow_south) = 
  (cellValue($row_south, 'column:avg_trip_length'),
  cellValue($row_south, 'column:trip_count'),
  cellValue($row_south, 'column:tmax'),
  cellValue($row_south, 'column:tmin'),
  cellValue($row_south, 'column:prcp'),
  cellValue($row_south, 'column:snow'));


# Print an HTML page with the table. Perl CGI has commands for all the
# common HTML tags
print header, start_html(-title=>'hello CGI',-head=>Link({-rel=>'stylesheet',-href=>'/ahmed/divvy/static/bootstrap.min.css',-type=>'text/css'}));

#northside table
print "North Side Table";
print p({-style=>"bottom-margin:10px"});
print table({-class=>'table table-hover', -style=>'width:90%;margin:auto;'},
  Tr([th(['Number of Trips', 'Avg Trip Time', 'Max Temp', 'Min Temp', 'Precipitation', 'Snowfall']),
  td([$ride_count_north,
  convert_time($average_ride_time_north),
  convert_c_to_f($tmax_north),
  convert_c_to_f($tmin_north),
  convert_mm_to_in($prcp_north) . ' inches ',
  convert_mm_to_in($snow_north) . ' inches'])])),
    p({-style=>"bottom-margin:10px"});

#southside table
print "South Side Table";
print p({-style=>"bottom-margin:10px"});
print table({-class=>'table table-hover', -style=>'width:90%;margin:auto;'},
  Tr([th(['Number of Trips', 'Avg Trip Time', 'Max Temp', 'Min Temp', 'Precipitation', 'Snowfall']),
  td([$ride_count_south,
  convert_time($average_ride_time_south),
  convert_c_to_f($tmax_south),
  convert_c_to_f($tmin_south),
  convert_mm_to_in($prcp_south) . ' inches ',
  convert_mm_to_in($snow_south) . ' inches'])])),
    p({-style=>"bottom-margin:10px"});


print end_html;
