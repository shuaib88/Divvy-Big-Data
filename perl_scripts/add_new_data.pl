#!/usr/bin/perl -w
# Program: cass_sample.pl
# Note: includes bug fixes for Net::Async::CassandraCQL 0.11 version

use strict;
use warnings;
use 5.10.0;
use FindBin;

use Scalar::Util qw(
        blessed
    );
use Try::Tiny;

use Kafka::Connection;
use Kafka::Producer;

use Data::Dumper;
use CGI qw/:standard/, 'Vars';

my $date = param('date');
if(!$date) {
    exit;
}

my ( $connection, $producer );
try {
    #-- Connection
    # $connection = Kafka::Connection->new( host => 'sandbox.hortonworks.com', port => 6667 );
    $connection = Kafka::Connection->new( host => 'hadoop-m.c.mpcs53013-2015.internal', port => 6667 );

    #-- Producer
    $producer = Kafka::Producer->new( Connection => $connection );
    # Only put in the station_id and weather elements because those are the only ones we care about
    my $message = "<new_trip-entry>";
	$message .= "<date>".param("date")."</date>";
	$message .= "<trip_time>".param("trip_time")."</trip_time>";
	$message .= "<max_temp>".param("max_temp")."</max_temp>";
	$message .= "<min_temp>".param("min_temp")."</min_temp>";
	$message .= "<precipitation>".param("precipitation")."</precipitation>";
	$message .= "<snow>".param("snow")."</snow>";
   	$message .= "</new_trip-entry>";


    # Sending a single message
    my $response = $producer->send(
	'ahmed_divvy',          # topic
	0,                                 # partition
	$message                           # message
        );
} catch {
    if ( blessed( $_ ) && $_->isa( 'Kafka::Exception' ) ) {
	warn 'Error: (', $_->code, ') ',  $_->message, "\n";
	exit;
    } else {
	die $_;
    }
};

# Closes the producer and cleans up
undef $producer;
undef $connection;

print header, start_html(-title=>'Submit weather',-head=>Link({-rel=>'stylesheet',-href=>'/table.css',-type=>'text/css'}));
print table({-class=>'CSS_Table_Example', -style=>'width:80%;'},
            caption('Weather report submitted'),
	    Tr([td(["date"]),
	        td([$date])]));

#print $protocol->getTransport->getBuffer;
print end_html;

