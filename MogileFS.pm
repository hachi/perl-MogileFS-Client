#!/usr/bin/perl
#
# MogileFS client library
#
# Copyright 2004 Danga Interactive
#
# Authors:
#    Brad Whitaker <whitaker@danga.com>
#    Brad Fitzpatrick <brad@danga.com>
#
# License:
#    GPL or Artistic License
#
# FIXME: add url to website (once we have one)
# FIXME: add POD docs (Michael?)
#

package MogileFS;

use strict;
use IO::Socket::INET;
use Carp;
use fields qw(root domain hosts host_dead lasterr lasterrstr);

sub new {
    my MogileFS $self = shift;
    $self = fields::new($self) unless ref $self;

    my %args = @_;

    # FIXME: add validation
    {
        $self->{root} = $args{root} or
            croak "MogileFS: constructor requires parameter 'root'";

        $self->{hosts} = $args{hosts} or
            croak "MogileFS: constructor requires parameter 'hosts'";

        $self->{domain} = $args{domain} or
            croak "MogileFS: constructor requires parameter 'domain'";
    }

    $self->{host_dead} = {}; # FIXME: keep a real hash of dead hosts

    debug("MogileFS object: ", $self);

    return $self;
}

# returns MogileFS::NewFile object, or undef if no device
# available for writing
sub new_file {
    my MogileFS $self = shift;
    my ($key, $class) = @_;
    croak "MogileFS: requires key" unless length($key);

    my $res = $self->_do_request("create_open", {
        domain => $self->{domain},
        class  => $class,
        key    => $key,
    }) or return undef;

    # create a MogileFS::NewFile object, based off of IO::File
    return MogileFS::NewFile->new(
                                  mg    => $self,
                                  fid   => $res->{fid},
                                  path  => $res->{path},
                                  devid => $res->{devid},
                                  class => $class,
                                  key   => $key
                                  );
}

sub debug {
    my $msg = shift;
    my $ref = shift;
    chomp $msg;

    use Data::Dumper;
    print STDERR "$msg";
    print STDERR Dumper($ref) if $ref;
    print STDERR "\n";

    return;
}

######################################################################
# Server communications
#

sub _sock_to_host { # (host)
    my $host = shift;

    # FIXME: do non-blocking IO
    return IO::Socket::INET->new(PeerAddr => $host,
                                 Proto    => 'tcp',
                                 Blocking => 1,
                                 );
}

sub _do_request {
    my MogileFS $self = shift;
    my ($cmd, $args) = @_;

    die "MogileFS: invalid arguments to _do_request"
        unless $cmd && $args;

    # FIXME: cache socket in $self, support multiple hosts
    my $sock = _sock_to_host($self->{hosts}->[0]);
    debug("SOCK: $sock");

    my $argstr = _encode_url_string(%$args);

    $sock->print("$cmd $argstr\r\n");
    debug("REQUEST: $cmd $argstr");

    my $line = <$sock>;
    debug("RESPONSE: $line");

    if ($line =~ /^ERR\s+(\w+)\s*(\S*)/) {
        $self->{'lasterr'} = $1;
        $self->{'lasterrstr'} = $2 || undef;
        debug("LASTERR: $1 $2");
        return undef;
    }

    # OK <arg_len> <response>
    if ($line =~ /^OK\s+\d*\s*(\S*)/) {
        my $args = _decode_url_string($1);
        debug("RETURN_VARS: ", $args);
        return $args;
    }

    croak("MogileFS: invalid response from server");
    return undef;
}

######################################################################
# Data encoding
#

sub _escape_url_string {
    my $str = shift;
    $str =~ s/([^a-zA-Z0-9_\,\-.\/\\\: ])/uc sprintf("%%%02x",ord($1))/eg;
    $str =~ tr/ /+/;
    return $str;
}

sub _encode_url_string {
    my %args = @_;
    return undef unless %args;
    return join("&",
                map { _escape_url_string($_) . '=' .
                      _escape_url_string($args{$_}) } 
                grep { defined $args{$_} } keys %args
                );
}

sub _decode_url_string {
    my $arg = shift;
    my $buffer = ref $arg ? $arg : \$arg;
    my $hashref = {};  # output hash

    my $pair;
    my @pairs = split(/&/, $$buffer);
    my ($name, $value);
    foreach $pair (@pairs) {
        ($name, $value) = split(/=/, $pair);
        $value =~ tr/+/ /;
        $value =~ s/%([a-fA-F0-9][a-fA-F0-9])/pack("C", hex($1))/eg;
        $name =~ tr/+/ /;
        $name =~ s/%([a-fA-F0-9][a-fA-F0-9])/pack("C", hex($1))/eg;
        $hashref->{$name} .= $hashref->{$name} ? "\0$value" : $value;
    }

    return $hashref;
}


######################################################################
# MogileFS::NewFile object
#

package MogileFS::NewFile;

use IO::File;
use base 'IO::File';

sub new {
    my MogileFS::NewFile $self = shift;
    my %args = @_;

    my $mg = $args{mg};
    # FIXME: validate %args

    my $file = "$mg->{root}/$args{path}";

    my $fh = new IO::File ">$file"
        or die "couldn't open: $file";

    my $attr = _get_attrs($fh);

    # prefix our keys with "mogilefs_newfile_" as per IO::Handle recommendations
    # and copy safe %args values into $attr
    $attr->{"mogilefs_newfile_$_"} = $args{$_} foreach qw(mg fid devid key path);

    return bless $fh;
}

sub delete {
    my MogileFS::NewFile $self = shift;

    my $attr = $self->_get_attrs;

    my $mg  = $attr->{mogilefs_newfile_mg};
    my $key = $attr->{mogilefs_newfile_key};
    my $domain = $mg->{domain};
    
    $mg->_do_request("delete", {
        domain => $domain,
        key    => $key,
    }) or return undef;

    return 1;
}

sub get_paths {
    my MogileFS::NewFile $self = shift;
    my %args = @_;

    my $mg  = $self->{mogilefs_newfile_mg};
    my $key = $self->{key};
    my $domain = $mg->{domain};

    my $res = $mg->_do_request("get_paths", {
        domain => $domain,
        key    => $key,
    }) or return undef;

    return map { $res->{"path$_"} } (1..$res->{paths});
}

sub close {
    my MogileFS::NewFile $self = shift;

    # actually close the file
    $self->SUPER::close;

    # get a reference to the hash part of the $fh typeglob ref
    my $attr = $self->_get_attrs;

    my $mg    = $attr->{mogilefs_newfile_mg};
    my $fid   = $attr->{mogilefs_newfile_fid};
    my $devid = $attr->{mogilefs_newfile_devid};
    my $path  = $attr->{mogilefs_newfile_path};
    my $key   = $attr->{mogilefs_newfile_key};
    my $domain = $mg->{domain};

    my MogileFS $mg = $attr->{mogilefs_newfile_mg};
    $mg->_do_request("create_close", {
        fid    => $fid,
        devid  => $devid,
        domain => $domain,
        key    => $key,
        path   => $path,
    }) or return undef;

    return 1;
}

# get a reference to the hash part of the $fh typeglob ref
sub _get_attrs {
    return \%{*{$_[0]}};
}


1;
