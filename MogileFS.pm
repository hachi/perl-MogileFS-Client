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
use IO::WrapTie;
use LWP::Simple;
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

sub errstr {
    my MogileFS $self = shift;
    return undef unless $self;

    return $self->{backend}->errstr;
}

# returns MogileFS::NewFile object, or undef if no device
# available for writing
sub new_file {
    my MogileFS $self = shift;
    my ($key, $class, $bytes) = @_;

    my $res = $self->{backend}->do_request
        ("create_open", {
            domain => $self->{domain},
            class  => $class,
            key    => $key,
        }) or return undef;

    # create a MogileFS::NewFile object, based off of IO::File
    if ($res->{path} =~ m!^http://!) {
        return IO::WrapTie::wraptie('MogileFS::NewHTTPFile', 
                                          mg    => $self,
                                          fid   => $res->{fid},
                                          path  => $res->{path},
                                          devid => $res->{devid},
                                          class => $class,
                                          key   => $key,
                                          content_length => $bytes+0,
                                          );
    } else {
        return MogileFS::NewFile->new(
                                      mg    => $self,
                                      fid   => $res->{fid},
                                      path  => $res->{path},
                                      devid => $res->{devid},
                                      class => $class,
                                      key   => $key
                                      );
    }
}

sub get_paths {
    my MogileFS $self = shift;
    my $key = shift;
    my $noverify = shift;

    my $res = $self->{backend}->do_request
        ("get_paths", {
            domain => $self->{domain},
            key    => $key,
            noverify => $noverify ? 1 : 0,
        }) or return undef;

    my @paths = map { $res->{"path$_"} } (1..$res->{paths});
    return @paths if scalar(@paths) > 0 && $paths[0] =~ m!^http://!;
    return map { "$self->{root}/$_"} @paths;
}

# given a key, returns a scalar reference pointing at a string containing
# the contents of the file. takes one parameter; a scalar key to get the
# data for the file.
sub get_file_data {
    # given a key, load some paths and get data
    my MogileFS $self = shift;
    my @paths = $self->get_paths(shift);

    # iterate over each
    foreach my $path (@paths) {
        if ($path =~ m!^http://!) {
            # try via HTTP
            my $contents = LWP::Simple::get($path);
            return \$contents if $contents;

        } else {
            # open the file from disk and just grab it all
            open FILE, "<$path" or next;
            my $contents;
            { local $/ = undef; $contents = <FILE>; }
            close FILE;
            return \$contents if $contents;
        }
    }
    return undef;
}

# TODO: delete method on MogileFS::NewFile object
# this method returns undef only on a fatal error such as inability to actually
# delete a resource and inability to contact the server.  attempting to delete
# something that doesn't exist counts as success, as it doesn't exist.
sub delete {
    my MogileFS $self = shift;
    my $key = shift;

    my $rv = $self->{backend}->do_request
        ("delete", {
            domain => $self->{domain},
            key    => $key,
        });

    # if it's unknown_key, not an error
    return undef unless defined $rv ||
                        $self->{backend}->{lasterr} eq 'unknown_key';

    return 1;
}

# just makes some sleeping happen.  first and only argument is number of
# seconds to instruct backend thread to sleep for.
sub sleep {
    my MogileFS $self = shift;
    my $duration = shift;

    $self->{backend}->do_request("sleep", { duration => $duration + 0 })
        or return undef;
    
    return 1;
}

# this method renames a file.  it returns an undef on error (only a fatal error
# is considered as undef; "file didn't exist" isn't an error).
sub rename {
    my MogileFS $self = shift;
    my ($fkey, $tkey) = @_;

    my $rv = $self->{backend}->do_request
        ("rename", {
            domain   => $self->{domain},
            from_key => $fkey,
            to_key   => $tkey,
        });

    # if it's unknown_key, not an error
    return undef unless defined $rv ||
                        $self->{backend}->{lasterr} eq 'unknown_key';

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
        # root is only needed for NFS based installations
        $self->{root} = $args{root};

        # get domain (required)
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
                     qw(hostid status hostname hostip http_port remoteroot) };
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

# get a hashref of statistics on how the MogileFS server is doing.  there are several
# sections of statistics, in this form:
# { 
#     replication => { "domain-name" => { "class-name" => { devcount => filecount }, ... }, ... },
# }
sub get_stats {
    my MogileFS::Admin $self = shift;
    my $ret = {};

    # do the request, default to request all stats if they didn't specify any
    push @_, 'all' unless @_;
    my $res = $self->{backend}->do_request("stats", { map { $_ => 1 } @_ })
        or return undef;

    # get replication statistics
    foreach my $i (1..$res->{"replicationcount"}) {
        $ret->{replication}->{$res->{"replication${i}domain"}}->{$res->{"replication${i}class"}}->{$res->{"replication${i}devcount"}} = $res->{"replication${i}files"};
    }

    # get file statistics
    foreach my $i (1..$res->{"filescount"}) {
        $ret->{files}->{$res->{"files${i}domain"}}->{$res->{"files${i}class"}} = $res->{"files${i}files"};
    }

    # get device statistics
    foreach my $i (1..$res->{"devicescount"}) {
        $ret->{devices}->{$res->{"devices${i}id"}} = {
            host => $res->{"devices${i}host"},
            status => $res->{"devices${i}status"},
            files => $res->{"devices${i}files"},
        };
    }

    # return the created response
    return $ret;
}

# get a hashref of the domains we know about in the format of
#   { domain_name => { class_name => mindevcount, class_name => mindevcount, ... }, ... }
sub get_domains {
    my MogileFS::Admin $self = shift;

    my $res = $self->{backend}->do_request("get_domains", {})
        or return undef;

    my $ret;
    foreach my $i (1..$res->{domains}) {
        $ret->{$res->{"domain$i"}} = {
            map {
                $res->{"domain${i}class${_}name"} =>
                    $res->{"domain${i}class${_}mindevcount"}
            } (1..$res->{"domain${i}classes"})
        };
    }

    return $ret;
}

# create a new domain
sub create_domain {
    my MogileFS::Admin $self = shift;
    my $domain = shift;

    my $res = $self->{backend}->do_request("create_domain", { domain => $domain });
    return undef unless $res->{domain} eq $domain;
    
    return 1;
}

# create a class within a domain
sub create_class {
    my MogileFS::Admin $self = shift;

    # wrapper around _mod_class(create)
    return $self->_mod_class(@_, 'create');
}


# update a class's mindevcount within a domain
sub update_class {
    my MogileFS::Admin $self = shift;

    # wrapper around _mod_class(update)
    return $self->_mod_class(@_, 'update');
}

# change the state of a device; pass in the hostname of the host the
# device is located on, the device id number, and the state you want
# the host to be set to.  returns 1 on success, undef on error.
sub change_device_state {
    my MogileFS::Admin $self = shift;
    my ($host, $device, $state) = @_;
    
    my $res = $self->{backend}->do_request("set_state", {
        host => $host,
        device => $device,
        state => $state,
    }) or return undef;
    
    return 1;
}


################################################################################
# MogileFS::Admin class methods
#

sub _fail {
    croak "MogileFS::Admin: $_[0]";
}

*_debug = *MogileFS::_debug;

# modify a class within a domain
sub _mod_class {
    my MogileFS::Admin $self = shift;
    my ($domain, $class, $mindevcount, $verb) = @_;
    $verb ||= 'create';
    
    my $res = $self->{backend}->do_request("${verb}_class", {
        domain => $domain,
        class => $class,
        mindevcount => $mindevcount,
    });
    return undef unless $res->{class} eq $class;
    
    return 1;
}


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
        or return _fail("couldn't connect to mogilefsd backend");
    _debug("SOCK: $sock");

    my $argstr = _encode_url_string(%$args);

    $sock->print("$cmd $argstr\r\n");
    _debug("REQUEST: $cmd $argstr");

    my $line = <$sock>;
    _debug("RESPONSE: $line");

    # ERR <errcode> <errstr>
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
    my MogileFS::Backend $self = shift;

    return join(" ", $self->{'lasterr'}, $self->{'lasterrstr'});
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

use strict;
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

################################################################################
# MogileFS::HTTPFile object
# NOTE: This is meant to be used within IO::WrapTie...
#

package MogileFS::NewHTTPFile;

use strict;
use Carp;

use fields ('host',
            'sock',           # IO::Socket; created only when we need it
            'uri',
            'data',           # buffered data we have
            'pos',            # simulated file position
            'length',         # length of data field
            'content_length', # declared length of data we will be receiving (not required)
            'mg',
            'fid',
            'devid',
            'class',
            'key',
            'path',           # full URL to save data to
            );

sub path  { _getset(shift, 'path');      }
sub class { _getset(shift, 'class', @_); }
sub key   { _getset(shift, 'key', @_);   }

sub TIEHANDLE {
    my MogileFS::NewHTTPFile $self = shift;
    $self = fields::new($self) unless ref $self;

    my %args = @_;
    return undef unless $args{path} =~ m!http://(.+?)(/.+)$!;

    $self->{host} = $1;
    $self->{uri} = $2;
    $self->{data} = '';
    $self->{length} = 0;
    $self->{content_length} = $args{content_length} + 0;
    $self->{pos} = 0;
    $self->{$_} = $args{$_} foreach qw(mg fid devid class key path);
    
    return $self;
}
*new = *TIEHANDLE;

sub PRINT {
    my MogileFS::NewHTTPFile $self = shift;

    # get data to send to server
    my $data = shift;
    my $newlen = length $data;
    $self->{pos} += $newlen;

    # now make socket if we don't have one
    if (!$self->{sock} && $self->{content_length}) {
        $self->{sock} = IO::Socket::INET->new(PeerAddr => $self->{host})
            or _fail("unable to open socket: $!");
        $self->{sock}->print("PUT $self->{uri} HTTP/1.0\r\nContent-length: $self->{content_length}\r\n\r\n");
    }

    # write some data to our socket
    if ($self->{sock}) {
        # store on data if we're under 1k
        if ($self->{length} < 1024) {
            if ($self->{length} + $newlen > 1024) {
                $self->{length} = 1024;
                $self->{data} .= substr($data, 0, 1024 - $self->{length});
            } else {
                $self->{length} += $newlen;
                $self->{data} .= $data;
            }
        }

        # actually write
        $self->{sock}->print($data);
    } else {
        # or not, just stick it on our queued data
        $self->{data} .= $data;
        $self->{length} += $newlen;
    }
}
*print = *PRINT;

sub CLOSE {
    my MogileFS::NewHTTPFile $self = shift;

    # if we're closed and we have no sock...
    unless ($self->{sock}) {
        $self->{sock} = IO::Socket::INET->new(PeerAddr => $self->{host})
            or _fail("unable to open socket: $!");
        $self->{sock}->print("PUT $self->{uri} HTTP/1.0\r\nContent-length: $self->{length}\r\n\r\n");
        $self->{sock}->print($self->{data});
    }
    
    # get response from put
    if ($self->{sock}) {
        my $line = $self->{sock}->getline;
        if ($line =~ m!^HTTP/\d+\.\d+\s+(\d+)!) {
            # all 2xx responses are success
            unless ($1 >= 200 && $1 <= 299) {
                # read through to the body
                my ($found_header, $body);
                while (defined (my $l = $self->{sock}->getline)) {
                    # remove trailing stuff
                    $l =~ s/[\r\n\s]+$//g;
                    $found_header = 1 unless $l;
                    next unless $found_header;

                    # add line to the body, with a space for readability
                    $body .= " $l";
                }
                $body = substr($body, 0, 512) if length $body > 512;
                $@ = "HTTP response $1 from upload: $body\n";
                return undef;
            }
        } else {
            $@ = "Response line not understood: $line\n";
            return undef;
        }
        $self->{sock}->close;
    }

    my MogileFS $mg = $self->{mg};
    my $domain = $mg->{domain};

    my $fid   = $self->{fid};
    my $devid = $self->{devid};
    my $path  = $self->{path};

    my $key = shift || $self->{key};

    $mg->{backend}->do_request
        ("create_close", {
            fid    => $fid,
            devid  => $devid,
            domain => $domain,
            size   => $self->{content_length} ? $self->{content_length} : $self->{length},
            key    => $key,
            path   => $path,
        }) or return undef;

    return 1;
}
*close = *CLOSE;

sub TELL {
    # return our current pos
    return $_[0]->{pos};
}
*tell = *TELL;

sub SEEK {
    # simply set pos...
    _fail("seek past end of file") if $_[1] > $_[0]->{length};
    $_[0]->{pos} = $_[1];
}
*seek = *SEEK;

sub EOF {
    return ($_[0]->{pos} >= $_[0]->{length}) ? 1 : 0;
}
*eof = *EOF;

sub BINMODE {
    # no-op, we're always in binary mode
}
*binmode = *BINMODE;

sub READ {
    my MogileFS::NewHTTPFile $self = shift;
    my $count = $_[1] + 0;
    
    my $max = $self->{length} - $self->{pos};
    $max = $count if $count < $max;

    $_[0] = substr($self->{data}, $self->{pos}, $max);
    $self->{pos} += $max;

    return $max;
}
*read = *READ;


################################################################################
# MogileFS::NewHTTPFile class methods
#

sub _fail {
    croak "MogileFS::NewHTTPFile: $_[0]";
}

*_debug = *MogileFS::debug;

sub _getset {
    my MogileFS::NewHTTPFile $self = shift;
    my $what = shift;
    
    if (@_) {
        # note: we're a TIEHANDLE interface, so we're not QUITE like a
        # normal class... our parameters tend to come in via an arrayref
        my $val = shift;
        $val = shift(@$val) if ref $val eq 'ARRAY';
        return $self->{$what} = $val;
    } else {
        return $self->{$what};
    }
}

1;
