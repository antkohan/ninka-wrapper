#!/usr/bin/perl

use strict;
use Switch;
use DBI;
use File::Temp;
use File::Find;
use File::Basename;

if(scalar(@ARGV) != 2){
    print STDERR "Incorrect number of arguments\n";
    print STDERR "Correct usage is: perl ninka-wrapper <tar-file> <database>\n";
    exit 1;
}

my ($file, $db) = @ARGV;
my $dbh = DBI->connect("DBI:SQLite:dbname=$db", "", "", {RaiseError => 1})
    or die $DBI::errstr;

$dbh->do("CREATE TABLE IF NOT EXISTS
          comments (filename TEXT, content TEXT)");
$dbh->do("CREATE TABLE IF NOT EXISTS 
          sentences (filename TEXT, content TEXT)");
$dbh->do("CREATE TABLE IF NOT EXISTS 
          goodsents (filename TEXT, content TEXT)");
$dbh->do("CREATE TABLE IF NOT EXISTS
          badsents (filename TEXT, content TEXT)");
$dbh->do("CREATE TABLE IF NOT EXISTS
          senttoks (filename TEXT, content TEXT)");
$dbh->do("CREATE TABLE IF NOT EXISTS
          licenses (filename TEXT, content TEXT)");

my $tempdir = File::Temp->newdir();
my $dirname = $tempdir->dirname;

print "***** Extracting file [$file] to temporary directory [$dirname] *****\n";
my $output = execute("tar -xvf '$file' --directory '$dirname'");

my @files;
find(
    sub { push @files, $File::Find::name unless -d; }, 
    $dirname
);

print "***** Beginning Execution of Ninka *****\n";
foreach my $file (@files) {
    print "Running ninka on file [$file]\n";
    $output = execute("perl ../ninka/ninka.pl '$file'");
}

my @ninkafiles;
find(
    sub {
	my $ext = getExtension($File::Find::name);
	if($ext =~ m/(comments|sentences|goodsent|badsent|senttok|license)$/){
	    push @ninkafiles, $File::Find::name;  
	}
    }, 
    $dirname
);

print "***** Entering Ninka Data into Database [$db]\n *****";
foreach my $file (@ninkafiles) {
    
    my $basefile = basename($file);
    my $rootfile = removeExtension($basefile);

    #Read entire file into a string
    open (my $fh, '<', $file) or die "Can't open file $!";
    my $filedata = do { local $/; <$fh> };

    my $sth;
    switch (getExtension($basefile)){
	case ".comments" {
	    print "Inserting [$basefile\] into table comments\n";
	    $sth = $dbh->prepare("INSERT INTO comments VALUES(?, ?)");
	}
	case ".sentences" {
	    print "Inserting [$basefile\] into table sentences\n";
	    $sth = $dbh->prepare("INSERT INTO sentences VALUES(?, ?)");
	}
	case ".goodsent" {
	    print "Inserting [$basefile\] into table goodsents\n";
	    $sth = $dbh->prepare("INSERT INTO goodsents VALUES(?, ?)");
	}
	case ".badsent" {
	    print "Inserting [$basefile\] into table goodsents\n";
	    $sth = $dbh->prepare("INSERT INTO badsents VALUES(?, ?)");
	}
	case ".senttok" {
	    print "Inserting [$basefile\] into table senttoks\n";
	    $sth = $dbh->prepare("INSERT INTO senttoks VALUES(?, ?)");
	}
	case ".license" {
	    print "Inserting [$basefile\] into table licenses\n";
	    $sth = $dbh->prepare("INSERT INTO licenses VALUES(?, ?)");
	}
    }
    $sth->bind_param(1, $rootfile);
    $sth->bind_param(2, $filedata);
    $sth->execute;
    close($fh);
}

$dbh->disconnect;

sub getExtension {
    my ($file) = @_;
    my $filename = basename($file);
    my ($ext) = $filename =~ /(\.[^.]+)$/;
    return $ext;
}

sub removeExtension {
    my ($file) = @_;
    (my $filename = $file) =~ s/\.[^.]+$//;
    return $filename;
}

sub execute {
    my ($command) = @_;
    my $output = `$command`;
    my $status = ($? >> 8);
    die "execution of [$command] failed: status [$status]\n" if ($status != 0);
    return $output;

}
