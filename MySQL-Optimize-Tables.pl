#!/usr/bin/perl -w
# optimize_tables.pl - Version 1.0
# Optimize fragmented tables in MySQL Databases
#
# Original idea from:
#       http://github.com/rackerhacker/MySQLTuner-perl
#       https://github.com/rackerhacker/MySQLTuner-perl/issues/8
#
# Scop's sql statements gave the idea of how to get the list
#  of fragmented tables in a MySQL database.
# For the latest updates,
# Git repository available at https://github.com/mnikhil-git/MySQLTuner-perl
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#
# Nikhil Mulley
# mnikhil#gmail.com
# https://github.com/mnikhil-git/MySQLTuner-perl/optimize-tables.pl
#
# Script's dependencies
#   uses Net::MySQL -- pure perl implementation of client interface to mysqld
#
use strict;
use warnings;
use diagnostics;
use Getopt::Long;
use Net::MySQL;
use Data::Dumper;

my $script_version = '1.0';

# set defaults
my %opt = (
"host"      => 0,
"socket"    => 0,
"port"      => 0,
"user"      => 0,
"pass"      => 0,
"list"      => 0,
"fix"       => 0,
"verbose"   => 0,
);

use vars qw ($fix_tables @mysql_dbs $mysql_handle $mysql_default_user
$mysql_default_port $mysql_default_db_login $mysql_use_port
%db_fragmented_tables);

# Get options from command line
GetOptions(\%opt,
'host=s',
'socket=s',
'port=i',
'user=s',
'pass=s',
'dblist=s@',
#               'exclude_dblist=s@',
'dball',
'list',
'fix',
'verbose',
'help',
);

if (defined $opt{'help'} && $opt{'help'} == 1) { usage(); }

sub usage {
    
    # usage of the command line with --help option
    print " ".
    "MySQL-Optimize-Tables - perl script version $script_version\n".
    "Usage:\n".
    "  --host <hostname> Connect to MySQL server and perform checks\n".
    "  --socket <socket> Use a socket connection \n".
    "  --port <port> Use a different port for connection. (Default port: 3306) \n".
    "  --user <username> Username to use for authentication \n".
    "  --pass <password> Password to use for authentication \n".
    "  --list List the fragmented tables. Default option for all databases\n".
    "  --fix  Fix the fragmented tables. Run Optimize table for the listed tables\n".
    "  --dblist  Comma seperated list of databases to check the list of their fragmented tables\n".
    "  --exclude_dblist  Comma seperated list of databases to exclude from the check\n".
    "  --dball  All databases to check. Default. \n".
    "  --output-file  Store output onto the file. Default: $0.log \n".
    "\n";
    exit;
}


sub initialize_variables {
    
    $fix_tables = 0;
    @mysql_dbs = ();
    $mysql_handle = ();
    
    $mysql_default_user = 'root';
    $mysql_default_port = 3306;
    $mysql_default_db_login = 'information_schema';
    $mysql_use_port = 0;
    %db_fragmented_tables = ();
    
    # if hostname is not specified use socket connection
if (defined $opt{'host'} && $opt{'host'} ne 0) {
            chomp($opt{'host'});
            $opt{'port'} = (defined($opt{'port'})) ? $mysql_default_port : $opt{'port'};
            $mysql_use_port = 1;
        }
elsif (defined($opt{'socket'} && $opt{'socket'} ne 0)) {
            chomp($opt{'socket'});
            $mysql_use_port = 0;
        }
        
if (defined $opt{'user'} && $opt{'user'} ne 0) {
            chomp($opt{'user'});
            $opt{'user'} = (defined($opt{'user'})) ? $mysql_default_port : $opt{'user'};
            
            if (! defined $opt{'pass'} && $opt{'pass'} eq "" ) { usage(); }
        }
        
        # default operation is to list the fragmented tables
        if (defined $opt{'list'} && $opt{'list'} != 1) { usage(); }
        $opt{'list'} = 1;
        
        # default store the output/list into log file.
if (defined $opt{'output-file'} && $opt{'output-file'} ne 0) {
            chomp($opt{'output-file'});
            } else {
            $opt{'output-file'} = 'MySQL-Optimize-Tables.log';
        }
        
        if (defined $opt{'fix'} && $opt{'fix'} == 1) { $fix_tables = 1; }
        
if (defined $opt{'dblist'} && $opt{'dblist'} ne 0) {
            @mysql_dbs = split(/,/, join(',', @{$opt{'dblist'}}));
            $opt{'dball'} = 0;
            } else {
            
            # default is to check for all the databases then.
            @mysql_dbs = ();
            $opt{'dball'} = 1;
        }
        
    }
    
    
    sub connect_db {
        
        my %mysql_conn_details = (
        'user'     => $mysql_default_user,
        'database' => $mysql_default_db_login,
        'password' => $opt{'pass'},
        );
        
        # use socket method if defined
        if (defined($mysql_use_port) && $mysql_use_port != 0) {
            $mysql_conn_details{'hostname'} = $opt{'host'};
            $mysql_conn_details{'port'} = $mysql_default_port;
            } else {
            $mysql_conn_details{'unixsocket'} = $opt{'socket'};
        }
        
        $mysql_handle = Net::MySQL->new(%mysql_conn_details);
        print "INFO| Connected to MySQL server.  \n" if $opt{'verbose'};
        
    }
    
    
    sub prepare_query {
        my $sql_query;
        # query to fetch the list of fragmented tables across the databases from information_schema
        $sql_query = 'SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES where  DATA_FREE > 0 ';
    if (! $opt{'dball'} && @mysql_dbs) {
            my $dbcnt = 0;
            $sql_query .= 'AND ( ';
            while ($dbcnt <= $#mysql_dbs) {
                $sql_query .= "TABLE_SCHEMA <=> \"$mysql_dbs[$dbcnt]\" ";
                $sql_query .= ' OR ' if $dbcnt != $#mysql_dbs;
                $dbcnt++;
            }
            $sql_query .= ') ';
            
        }
        
        $sql_query .= 'ORDER BY TABLE_SCHEMA ASC, TABLE_NAME ASC';
        
        return($sql_query);
        
    }
    
    sub fetch_fragmented_tables {
        my $sql_query = &prepare_query;
        #print "INFO| SQL Query :$sql_query\n" if $opt{'verbose'};
        
        $mysql_handle->query(qq{$sql_query});
        if ($mysql_handle->has_selected_record) {
            my $db_record_iter = $mysql_handle->create_record_iterator;
            my $db_row;
            while( $db_row = $db_record_iter->each ) {
                push(@{$db_fragmented_tables{"$db_row->[0]"}}, $db_row->[1]);
            }
            
        }
    }
    
    sub display_fragmented_tables_summary {
        &fetch_fragmented_tables;
        if (defined (%db_fragmented_tables) ) {
            print "Summary of fragmented tables on $opt{host} MySQL database\n";
            print "=" x 75; print "\n";
            
            foreach my $db_schema (keys %db_fragmented_tables) {
                printf("%-20s : %-3d\n", $db_schema, scalar(@{$db_fragmented_tables{$db_schema}}));
            }
            print "=" x 75; print "\n";
            } else {
            print "INFO| No Fragmented tables found in database(s) \n" if $opt{'verbose'};
        }
    }
    
    sub fix_fragmented_tables {
        print "INFO| Running fix mode\n" if $opt{'verbose'};
        my $optimize_query = ();
        my $batch_limit = 5;
        
        foreach my $db_schema (keys %db_fragmented_tables) {
            print "INFO| Optimizing tables under \"$db_schema\" database \n" if $opt{'verbose'};
            #print "$db_schema:". join(',', @{$db_fragmented_tables{$db_schema}});
    while (my @batch = splice(@{$db_fragmented_tables{$db_schema}}, 0 , $batch_limit)) {
                $optimize_query = "OPTIMIZE TABLE  ";
                my $tbl_iter = 0;
                while ($tbl_iter <= $#batch) {
                    $optimize_query .= "$db_schema."."$batch[$tbl_iter]";
                    $optimize_query .= ", " if $tbl_iter != $#batch;
                    $tbl_iter++;
                }
                print "INFO| Executing SQL Query: $optimize_query\n" if $opt{'verbose'};
                
                $mysql_handle->query(qq{$optimize_query});
                sleep(1);
            }
        }
        
    }
    
    # -----------------------------------------------------------------------------
    # BEGIN 'MAIN'
    # -----------------------------------------------------------------------------
    #print "\n >> MySQL-Optimize-Tables  - Author: Nikhil Mulley ";
    #print "\n >>   Run with --help for additional options\n";
    
    initialize_variables;
    connect_db;
    display_fragmented_tables_summary;
    fix_fragmented_tables if defined($opt{'fix'}) && $opt{'fix'};
    
    if(defined($mysql_handle)) {
        # close the mysql connection!
        $mysql_handle->close;
    }

