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

################################################################################
# MogileFS class
#
package MogileFS;

use strict;
use Carp;
use fields qw(root domain backend);

sub new {
    my MogileFS $self = shift;
    $self = fields::new($self) unless ref $self;

    return $self->_init(@_);
}

sub reload {
    my MogileFS $self = shift;
    return undef unless $self;

    return $self->_init(@_);
}

# returns MogileFS::NewFile object, or undef if no device
# available for writing
sub new_file {
    my MogileFS $self = shift;
    my ($key, $class) = @_;

    my $res = $self->{backend}->do_request
        ("create_open", {
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

sub get_paths {
    my MogileFS $self = shift;
    my $key = shift;

    my $res = $self->{backend}->do_request
        ("get_paths", {
            domain => $self->{domain},
            key    => $key,
        }) or return undef;

    return map { "$self->{root}/" . $res->{"path$_"} } (1..$res->{paths});
}

# TODO: delete method on MogileFS::NewFile object
sub delete {
    my MogileFS $self = shift;
    my $key = shift;

    $self->{backend}->do_request
        ("delete", {
            domain => $self->{domain},
            key    => $key,
        }) or return undef;

    return 1;
}

################################################################################
# MogileFS class methods
#

sub _fail {
    croak "MogileFS: $_[0]";
}

sub _debug {
    return 1 unless $MogileFS::DEBUG;

    my $msg = shift;
    my $ref = shift;
    chomp $msg;

    use Data::Dumper;
    print STDERR "$msg\n" . Dumper($ref) . "\n";
    return 1;
}

sub _init {
    my MogileFS $self = shift;

    my %args = @_;

    # FIXME: add actual validation
    {
        $self->{root} = $args{root} or
            _fail("constructor requires parameter 'root'");

        $self->{domain} = $args{domain} or
            _fail("constructor requires parameter 'domain'");
        
        # create a new backend object if there's not one already,
        # otherwise call a reload on the existing one
        if ($self->{backend}) {
            $self->{backend}->reload( hosts => $args{hosts} );
        } else {
            $self->{backend} = MogileFS::Backend->new( hosts => $args{hosts} );
        }
        _fail("cannot instantiate MogileFS::Backend") unless $self->{backend};
    }

    _debug("MogileFS object: [$self]", $self);

    return $self;
}


################################################################################
# MogileFS::Admin class
#
package MogileFS::Admin;

use strict;
use Carp;
use fields qw(backend);

sub new {
    my MogileFS::Admin $self = shift;
    $self = fields::new($self) unless ref $self;

    my %args = @_;

    $self->{backend} = new MogileFS::Backend ( hosts => $args{hosts} )
        or _fail("couldn't instantiate MogileFS::Backend");

    return $self;
}

sub get_hosts {
    my MogileFS::Admin $self = shift;
    my $hostid = shift;

    my $args = $hostid ? { hostid => $hostid } : {};
    my $res = $self->{backend}->do_request("get_hosts", $args)
        or return undef;

    my @ret = ();
    foreach my $ct (1..$res->{hosts}) {
        push @ret, { map { $_ => $res->{"host${ct}_$_"} } 
                     qw(hostid status hostname hostip remoteroot) };
    }

    return \@ret;
}

sub get_devices {
    my MogileFS::Admin $self = shift;
    my $devid = shift;

    my $args = $devid ? { devid => $devid } : {};
    my $res = $self->{backend}->do_request("get_devices", $args)
        or return undef;

    my @ret = ();
    foreach my $ct (1..$res->{devices}) {
        push @ret, { (map { $_ => $res->{"dev${ct}_$_"} } qw(devid hostid status)),
                     (map { $_ => $res->{"dev${ct}_$_"}+0 } qw(mb_total mb_used)) };
    }

    return \@ret;

}


################################################################################
# MogileFS::Admin class methods
#

sub _fail {
    croak "MogileFS::Admin: $_[0]";
}

*_debug = *MogileFS::_debug;


######################################################################
# MogileFS::Backend class
#
package MogileFS::Backend;

use strict;
use Carp;
use IO::Socket::INET;
use fields qw(hosts host_dead lasterr lasterrstr);

sub new {
    my MogileFS::Backend $self = shift;
    $self = fields::new($self) unless ref $self;

    return $self->_init(@_);
}

sub reload {
    my MogileFS::Backend $self = shift;
    return undef unless $self;

    return $self->_init(@_);
}

sub do_request {
    my MogileFS::Backend $self = shift;
    my ($cmd, $args) = @_;

    _fail("invalid arguments to do_request")
        unless $cmd && $args;

    # FIXME: cache socket in $self?
    my $sock = $self->_get_sock
        or return undef;
    _debug("SOCK: $sock");

    my $argstr = _encode_url_string(%$args);

    $sock->print("$cmd $argstr\r\n");
    _debug("REQUEST: $cmd $argstr");

    my $line = <$sock>;
    _debug("RESPONSE: $line");

    if ($line =~ /^ERR\s+(\w+)\s*(\S*)/) {
        $self->{'lasterr'} = $1;
        $self->{'lasterrstr'} = $2 || undef;
        _debug("LASTERR: $1 $2");
        return undef;
    }

    # OK <arg_len> <response>
    if ($line =~ /^OK\s+\d*\s*(\S*)/) {
        my $args = _decode_url_string($1);
        _debug("RETURN_VARS: ", $args);
        return $args;
    }

    _fail("invalid response from server");
    return undef;
}

sub errstr {
    # FIXME: return lasterr - lasterrstr?
}


################################################################################
# MogileFS::Backend class methods
#

sub _fail {
    croak "MogileFS::Backend: $_[0]";
}

*_debug = *MogileFS::_debug;

sub _sock_to_host { # (host)
    my $host = shift;

    # FIXME: do non-blocking IO
    return IO::Socket::INET->new(PeerAddr => $host,
                                 Proto    => 'tcp',
                                 Blocking => 1,
                                 Timeout  => 1, # 1 sec?
                                 );
}

sub _init {
    my MogileFS::Backend $self = shift;

    my %args = @_;

    # FIXME: add actual validation
    {
        $self->{hosts} = $args{hosts} or
            _fail("constructor requires parameter 'hosts'");

        _fail("'hosts' argument must be an arrayref")
            unless ref $self->{hosts} eq 'ARRAY';

        _fail("'hosts' argument must be of form: 'host:port'")
            if grep(! /:\d+$/, @{$self->{hosts}});
    }

    $self->{host_dead} = {};

    return $self;
}

sub _get_sock {
    my MogileFS::Backend $self = shift;
    return undef unless $self;

    my $size = scalar(@{$self->{hosts}});
    my $tries = $size > 15 ? 15 : $size;
    my $idx = int(rand() * $size);

    my $now = time();
    my $sock;
    foreach (1..$tries) {
        my $host = $self->{hosts}->[$idx++ % $size];

        # try dead hosts every 30 seconds
        next if $self->{host_dead}->{$host} > $now-30;

        last if $sock = _sock_to_host($host);

        # mark sock as dead
        _debug("marking host dead: $host @ $now");
        $self->{host_dead}->{$host} = $now;
    }

    return $sock;
}

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
use Carp;

sub new {
    my MogileFS::NewFile $self = shift;
    my %args = @_;

    my $mg = $args{mg};
    # FIXME: validate %args

    my $file = "$mg->{root}/$args{path}";

    # Note: we open the file for read/write (+) with clobber (>) because
    # although we mostly want to just clobber it and start afresh, some
    # modules later on (in our case, Image::Size) may want to seek around
    # in the file and do some reads.
    my $fh = new IO::File "+>$file"
        or _fail("couldn't open: $file: $!");

    my $attr = _get_attrs($fh);

    # prefix our keys with "mogilefs_newfile_" as per IO::Handle recommendations
    # and copy safe %args values into $attr
    $attr->{"mogilefs_newfile_$_"} = $args{$_} foreach qw(mg fid devid key path);

    return bless $fh;
}

# some getter/setter functions
sub path  { return _getset(shift, "path"); }
sub key   { return _getset(shift, "key",   @_); }
sub class { return _getset(shift, "class", @_); }

sub close {
    my MogileFS::NewFile $self = shift;

    # actually close the file
    $self->SUPER::close;

    # get a reference to the hash part of the $fh typeglob ref
    my $attr = $self->_get_attrs;

    my MogileFS $mg = $attr->{mogilefs_newfile_mg};
    my $domain = $mg->{domain};

    my $fid   = $attr->{mogilefs_newfile_fid};
    my $devid = $attr->{mogilefs_newfile_devid};
    my $path  = $attr->{mogilefs_newfile_path};

    my $key = shift || $attr->{mogilefs_newfile_key};

    $mg->{backend}->do_request
        ("create_close", {
            fid    => $fid,
            devid  => $devid,
            domain => $domain,
            key    => $key,
            path   => $path,
        }) or return undef;

    return 1;
}

################################################################################
# MogileFS::NewFile class methods
#

sub _fail {
    croak "MogileFS::NewFile: $_[0]";
}

*_debug = *MogileFS::debug;

# get a reference to the hash part of the $fh typeglob ref
sub _get_attrs {
    return \%{*{$_[0]}};
}

sub _getset {
    my MogileFS::NewFile $self = shift;
    my $item = shift;

    my $attrs = $self->_get_attrs;

    # we're a setter
    if (@_) {
        return $attrs->{"mogilefs_newfile_$item"} = shift;
    }

    # we're a getter
    return $attrs->{"mogilefs_newfile_$item"};
}

1;
