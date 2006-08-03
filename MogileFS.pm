#!/usr/bin/perl
#
# MogileFS client library
#
# Copyright 2004 Danga Interactive
#
# Authors:
#    Brad Whitaker <whitaker@danga.com>
#    Brad Fitzpatrick <brad@danga.com>
#    Mark Smith <marksmith@danga.com>
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
use LWP::UserAgent;
use fields qw(root domain backend readonly);

our $AUTOLOAD;

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

sub readonly {
    my MogileFS $self = shift;
    return $self->{readonly} = $_[0] ? 1 : 0 if @_;
    return $self->{readonly};
}

# expects as argument a hashref of "standard-ip" => "preferred-ip"
sub set_pref_ip {
    my MogileFS $self = shift;
    $self->{backend}->set_pref_ip(shift)
        if $self->{backend};
}

# returns MogileFS::NewFile object, or undef if no device
# available for writing
# ARGS: ( key, class, bytes?, opts? )
# where bytes is optional and the length of the file and opts is also optional
# and is a hashref of options.  supported options: fid = unique file id to use
# instead of just picking one in the database.
sub new_file {
    my MogileFS $self = shift;
    return undef if $self->{readonly};

    my ($key, $class, $bytes, $opts) = @_;
    $bytes += 0;
    $opts ||= {};

    my $res = $self->{backend}->do_request
        ("create_open", {
            domain => $self->{domain},
            class  => $class,
            key    => $key,
            fid    => $opts->{fid} || 0, # fid should be specified, or pass 0 meaning to auto-generate one
            multi_dest => 1,
        }) or return undef;

    my $dests = [];  # [ [devid,path], [devid,path], ... ]

    # determine old vs. new format to populate destinations
    unless (exists $res->{dev_count}) {
        push @$dests, [ $res->{devid}, $res->{path} ];
    } else {
        for my $i (1..$res->{dev_count}) {
            push @$dests, [ $res->{"devid_$i"}, $res->{"path_$i"} ];
        }
    }

    my $main_dest = shift @$dests;
    my ($main_devid, $main_path) = ($main_dest->[0], $main_dest->[1]);

    # create a MogileFS::NewFile object, based off of IO::File
    if ($main_path =~ m!^http://!) {
        return IO::WrapTie::wraptie('MogileFS::NewHTTPFile',
                                    mg    => $self,
                                    fid   => $res->{fid},
                                    path  => $main_path,
                                    devid => $main_devid,
                                    backup_dests => $dests,
                                    class => $class,
                                    key   => $key,
                                    content_length => $bytes+0,
                                    );
    } else {
        return MogileFS::NewFile->new(
                                      mg    => $self,
                                      fid   => $res->{fid},
                                      path  => $main_path,
                                      devid => $main_devid,
                                      class => $class,
                                      key   => $key
                                      );
    }
}

# Wrapper around new_file, print, and close.
# Given a key, class, and a filehandle or filename, stores the
# file contents in MogileFS. Returns the number of bytes stored on
# success, undef on failure.
sub store_file {
    my MogileFS $self = shift;
    return undef if $self->{readonly};

    my($key, $class, $file, $opts) = @_;
    my $fh = $self->new_file($key, $class, undef, $opts) or return;
    my $fh_from;
    if (ref($file)) {
        $fh_from = $file;
    } else {
        open $fh_from, $file or return;
    }
    my $bytes;
    while (my $len = read $fh_from, my($chunk), 8192) {
        $fh->print($chunk);
        $bytes += $len;
    }
    close $fh_from unless ref $file;
    $fh->close or return;
    $bytes;
}

# Wrapper around new_file, print, and close.
# Given a key, class, and file contents (scalar or scalarref), stores the
# file contents in MogileFS. Returns the number of bytes stored on
# success, undef on failure.
sub store_content {
    my MogileFS $self = shift;
    return undef if $self->{readonly};

    my($key, $class, $content, $opts) = @_;
    my $fh = $self->new_file($key, $class, undef, $opts) or return;
    $content = ref($content) eq 'SCALAR' ? $$content : $content;
    $fh->print($content);
    $fh->close or return;
    length($content);
}

# old style calling:
#   get_paths(key, noverify)
# new style calling:
#   get_paths(key, { noverify => 0/1, zone => "zone" });
# but with both, second parameter is optional
#
# returns list of URLs that key can be found at, or the empty
# list on either error or no paths
sub get_paths {
    my MogileFS $self = shift;
    my ($key, $opts) = @_;

    # handle parameters, if any
    my ($noverify, $zone);
    if (ref $opts) {
        $noverify = 1 if $opts->{noverify};
        $zone = $opts->{zone} || undef;
    } else {
        $noverify = 1 if $opts;
    }

    my $res = $self->{backend}->do_request
        ("get_paths", {
            domain => $self->{domain},
            key    => $key,
            noverify => $noverify ? 1 : 0,
            zone   => $zone,
        }) or return ();

    my @paths = map { $res->{"path$_"} } (1..$res->{paths});
    return @paths if scalar(@paths) > 0 && $paths[0] =~ m!^http://!;
    return map { "$self->{root}/$_"} @paths;
}

# given a key, returns a scalar reference pointing at a string containing
# the contents of the file. takes one parameter; a scalar key to get the
# data for the file.
sub get_file_data {
    # given a key, load some paths and get data
    my MogileFS $self = $_[0];
    my ($key, $timeout) = ($_[1], $_[2]);

    my @paths = $self->get_paths($key, 1);
    return undef unless @paths;

    # iterate over each
    foreach my $path (@paths) {
        next unless defined $path;
        if ($path =~ m!^http://!) {
            # try via HTTP
            my $ua = new LWP::UserAgent;
            $ua->timeout($timeout || 10);

            my $res = $ua->get($path);
            if ($res->is_success) {
                my $contents = $res->content;
                return \$contents;
            }

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
    return undef if $self->{readonly};

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
    return undef if $self->{readonly};

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

# used to get a list of keys matching a certain prefix.  expected arguments:
#   ( $prefix, $after, $limit )
# prefix specifies what you want to get a list of.  after is the item specified
# as a return value from this function last time you called it.  limit is optional
# and defaults to 1000 keys returned.
#
# if you expect an array of return values, returns:
#   ($after, $keys)
# but if you expect only a single value, you just get the arrayref of keys.  the
# value $after is to be used as $after when you call this function again.
#
# when there are no more keys in the list, you will get back undef(s).
sub list_keys {
    my MogileFS $self = shift;
    my ($prefix, $after, $limit) = @_;

    my $res = $self->{backend}->do_request
        ("list_keys", {
            domain => $self->{domain},
            prefix => $prefix,
            after => $after,
            limit => $limit,
        }) or return undef;

    # construct our list of keys and the new after value
    my $resafter = $res->{next_after};
    my $reslist = [];
    for (my $i = 1; $i <= $res->{key_count}+0; $i++) {
        push @$reslist, $res->{"key_$i"};
    }
    return wantarray ? ($resafter, $reslist) : $reslist;
}

# used to support plugins that have modified the server, this builds things into
# an argument list and passes them back to the server
# TODO: there is no readonly protection here?  does it matter?  should we check
# with the server to see what methods they support?  (and if they should be disallowed
# when the client is in readonly mode?)
sub AUTOLOAD {
    my MogileFS $self = shift;

    # remove everything up to the last colon, so we only have the method name left
    my $method = $AUTOLOAD;
    $method =~ s/^.*://; 

    # let this work
    no strict 'refs';

    # create a method to pass this on back
    *{$AUTOLOAD} = sub {
        # pre-assemble the arguments into a hashref
        my $ct = 0;
        my $args = {};
        $args->{"arg" . ++$ct} = shift() while @_;
        $args->{"argcount"} = $ct;

        # now call and return whatever we get back from the backend
        return $self->{backend}->do_request("plugin_$method", $args);
    };

    # now pass through
    goto &$AUTOLOAD;
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

    eval "use Data::Dumper;";
    print STDERR "$msg\n" . Dumper($ref) . "\n";
    return 1;
}

sub _init {
    my MogileFS $self = shift;

    my %args = @_;

    # FIXME: add actual validation
    {
        # by default, set readonly off
        $self->{readonly} = $args{readonly} ? 1 : 0;

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
            $self->{backend} = MogileFS::Backend->new( hosts => $args{hosts},
                                                       timeout => $args{timeout},
                                                       );
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
use fields qw(backend readonly);

sub new {
    my MogileFS::Admin $self = shift;
    $self = fields::new($self) unless ref $self;

    my %args = @_;

    $self->{readonly} = $args{readonly} ? 1 : 0;
    $self->{backend} = new MogileFS::Backend ( hosts => $args{hosts} )
        or _fail("couldn't instantiate MogileFS::Backend");

    return $self;
}

sub readonly {
    my MogileFS::Admin $self = shift;
    return $self->{readonly} = $_[0] ? 1 : 0 if @_;
    return $self->{readonly};
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
                     qw(hostid status hostname hostip http_port http_get_port remoteroot altip altmask) };
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
                     (map { $_ => $res->{"dev${ct}_$_"}+0 } qw(mb_total mb_used weight)) };
    }

    return \@ret;

}

# get raw information about fids, for enumerating the dataset
#   ( $from_fid, $to_fid )
# returns:
#   { fid => { hashref with keys: domain, class, devcount, length, key } }
sub list_fids {
    my MogileFS::Admin $self = shift;
    my ($fromfid, $tofid) = @_;

    my $res = $self->{backend}->do_request('list_fids', { from => $fromfid, to => $tofid })
        or return undef;

    my $ret = {};
    foreach my $i (1..$res->{fid_count}) {
        $ret->{$res->{"fid_${i}_fid"}} = {
            key => $res->{"fid_${i}_key"},
            length => $res->{"fid_${i}_length"},
            class => $res->{"fid_${i}_class"},
            domain => $res->{"fid_${i}_domain"},
            devcount => $res->{"fid_${i}_devcount"},
        };
    }
    return $ret;
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

    # get fid statistics if they're provided
    if ($res->{fidmax}) {
        $ret->{fids} = {
            max => $res->{fidmax},
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

    my $ret = {};
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
    return undef if $self->{readonly};

    my $domain = shift;

    my $res = $self->{backend}->do_request("create_domain", { domain => $domain });
    return undef unless $res->{domain} eq $domain;

    return 1;
}

# delete a domain
sub delete_domain {
    my MogileFS::Admin $self = shift;
    return undef if $self->{readonly};

    my $domain = shift;

    $self->{backend}->do_request("delete_domain", { domain => $domain })
        or return undef;

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

# delete a class
sub delete_class {
    my MogileFS::Admin $self = shift;
    return undef if $self->{readonly};

    my ($domain, $class) = @_;

    $self->{backend}->do_request("delete_class", {
            domain => $domain,
            class => $class,
        }) or return undef;

    return 1;
}


# create a host
sub create_host {
    my MogileFS::Admin $self = shift;
    my $host = shift;
    return undef unless $host;

    my $args = shift;
    return undef unless ref $args eq 'HASH';
    return undef unless $args->{ip} && $args->{port};

    return $self->_mod_host($host, $args, 'create');
}

# edit a host
sub update_host {
    my MogileFS::Admin $self = shift;
    my $host = shift;
    return undef unless $host;

    my $args = shift;
    return undef unless ref $args eq 'HASH';

    return $self->_mod_host($host, $args, 'update');
}

# delete a host
sub delete_host {
    my MogileFS::Admin $self = shift;
    return undef if $self->{readonly};

    my $host = shift;
    return undef unless $host;

    $self->{backend}->do_request("delete_host", { host => $host })
        or return undef;
    return 1;
}

# create a new device
sub create_device {
    my MogileFS::Admin $self = shift;
    return undef if $self->{readonly};

    my (%opts) = @_;   #hostname or hostid, devid, state (optional)

    my $res = $self->{backend}->do_request("create_device", \%opts)
        or return undef;

    return 1;
}

# change the state of a device; pass in the hostname of the host the
# device is located on, the device id number, and the state you want
# the host to be set to.  returns 1 on success, undef on error.
sub change_device_state {
    my MogileFS::Admin $self = shift;
    return undef if $self->{readonly};

    my ($host, $device, $state) = @_;

    my $res = $self->{backend}->do_request("set_state", {
        host => $host,
        device => $device,
        state => $state,
    }) or return undef;

    return 1;
}

# change the weight of a device by passing in the hostname and
# the device id
sub change_device_weight {
    my MogileFS::Admin $self = shift;
    return undef if $self->{readonly};

    my ($host, $device, $weight) = @_;
    $weight += 0;

    my $res = $self->{backend}->do_request("set_weight", {
        host => $host,
        device => $device,
        weight => $weight,
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
    return undef if $self->{readonly};

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

# modify a host
sub _mod_host {
    my MogileFS::Admin $self = shift;
    return undef if $self->{readonly};

    my ($host, $args, $verb) = @_;

    $args ||= {};
    $args->{host} = $host;
    $verb ||= 'create';

    my $res = $self->{backend}->do_request("${verb}_host", $args);
    return undef unless $res->{host} eq $host;

    return 1;
}

sub errstr {
    my MogileFS::Admin $self = shift;
    return undef unless $self->{backend};
    return $self->{backend}->errstr;
}

sub errcode {
    my MogileFS::Admin $self = shift;
    return undef unless $self->{backend};
    return $self->{backend}->errcode;
}

sub err {
    my MogileFS::Admin $self = shift;
    return undef unless $self->{backend};
    return $self->{backend}->err;
}

######################################################################
# MogileFS::Backend class
#
package MogileFS::Backend;

use strict;
no strict 'refs';

use Carp;
use IO::Socket::INET;
use Socket qw( MSG_NOSIGNAL PF_INET IPPROTO_TCP SOCK_STREAM );
use Errno qw( EINPROGRESS EWOULDBLOCK EISCONN );
use POSIX ();

use fields ('hosts',        # arrayref of "$host:$port" of mogilefsd servers
            'host_dead',    # "$host:$port" -> $time  (of last connect failure)
            'lasterr',      # string: \w+ identifer of last error
            'lasterrstr',   # string: english of last error
            'sock_cache',   # cached socket to mogilefsd tracker
            'pref_ip',      # hashref; { ip => preferred ip }
            'timeout',      # time in seconds to allow sockets to become readable
            );

use vars qw($FLAG_NOSIGNAL $PROTO_TCP);
eval { $FLAG_NOSIGNAL = MSG_NOSIGNAL; };

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

sub set_pref_ip {
    my MogileFS::Backend $self = shift;
    $self->{pref_ip} = shift;
    $self->{pref_ip} = undef
        unless $self->{pref_ip} &&
               ref $self->{pref_ip} eq 'HASH';
}

sub _wait_for_readability {
    my ($fileno, $timeout) = @_;
    return 0 unless $fileno && $timeout;

    my $rin = '';
    vec($rin, $fileno, 1) = 1;
    my $nfound = select($rin, undef, undef, $timeout);

    # undef/0 are failure, 1 is success
    return $nfound ? 1 : 0;
}

sub do_request {
    my MogileFS::Backend $self = shift;
    my ($cmd, $args) = @_;

    _fail("invalid arguments to do_request")
        unless $cmd && $args;

    local $SIG{'PIPE'} = "IGNORE" unless $FLAG_NOSIGNAL;

    my $sock = $self->{sock_cache};
    my $argstr = _encode_url_string(%$args);
    my $req = "$cmd $argstr\r\n";
    my $reqlen = length($req);
    my $rv = 0;

    if ($sock) {
        # try our cached one, but assume it might be bogus
        _debug("SOCK: cached = $sock, REQ: $req");
        $rv = send($sock, $req, $FLAG_NOSIGNAL);
        if ($! || ! defined $rv) {
            # undef is error, but $! may not be populated, we've found
            undef $self->{sock_cache};
        } elsif ($rv != $reqlen) {
            return _fail("send() didn't return expected length ($rv, not $reqlen)");
        }
    }

    unless ($rv) {
        $sock = $self->_get_sock
            or return _fail("couldn't connect to mogilefsd backend");
        _debug("SOCK: $sock, REQ: $req");
        $rv = send($sock, $req, $FLAG_NOSIGNAL);
        if ($!) {
            return _fail("error talking to mogilefsd tracker: $!");
        } elsif ($rv != $reqlen) {
            return _fail("send() didn't return expected length ($rv, not $reqlen)");
        }
        $self->{sock_cache} = $sock;
    }

    # wait up to 3 seconds for the socket to come to life
    unless (_wait_for_readability(fileno($sock), $self->{timeout})) {
        close($sock);
        return _fail("socket never became readable");
    }

    # guard against externally-modified $/ changes.  patch from
    # Andreas J. Koenig.  in practice nobody should do this, though,
    # and this line should be unnecessary.
    local $/ = "\n";

    my $line = <$sock>;
    _debug("RESPONSE: $line");
    return _fail("socket closed on read")
        unless defined $line;

    # ERR <errcode> <errstr>
    if ($line =~ /^ERR\s+(\w+)\s*(\S*)/) {
        $self->{'lasterr'} = $1;
        $self->{'lasterrstr'} = $2 ? _unescape_url_string($2) : undef;
        _debug("LASTERR: $1 $2");
        return undef;
    }

    # OK <arg_len> <response>
    if ($line =~ /^OK\s+\d*\s*(\S*)/) {
        my $args = _decode_url_string($1);
        _debug("RETURN_VARS: ", $args);
        return $args;
    }

    _fail("invalid response from server: [$line]");
    return undef;
}

sub errstr {
    my MogileFS::Backend $self = shift;

    return join(" ", $self->{'lasterr'}, $self->{'lasterrstr'});
}

sub errcode {
    my MogileFS::Backend $self = shift;
    return $self->{lasterr};
}

sub err {
    my MogileFS::Backend $self = shift;
    return $self->{lasterr} ? 1 : 0;
}

################################################################################
# MogileFS::Backend class methods
#

sub _fail {
    croak "MogileFS::Backend: $_[0]";
}

*_debug = *MogileFS::_debug;

sub _connect_sock { # sock, sin, timeout
    my ($sock, $sin, $timeout) = @_;
    $timeout ||= 0.25;

    # make the socket non-blocking for the connection if wanted, but
    # unconditionally set it back to blocking mode at the end

    if ($timeout) {
        IO::Handle::blocking($sock, 0);
    } else {
        IO::Handle::blocking($sock, 1);
    }

    my $ret = connect($sock, $sin);

    if (!$ret && $timeout && $!==EINPROGRESS) {

        my $win='';
        vec($win, fileno($sock), 1) = 1;

        if (select(undef, $win, undef, $timeout) > 0) {
            $ret = connect($sock, $sin);
            # EISCONN means connected & won't re-connect, so success
            $ret = 1 if !$ret && $!==EISCONN;
        }
    }

    # turn blocking back on, as we expect to do blocking IO on our sockets
    IO::Handle::blocking($sock, 1) if $timeout;

    return $ret;
}

sub _sock_to_host { # (host)
    my MogileFS::Backend $self = shift;
    my $host = shift;

    # create a socket and try to do a non-blocking connect
    my ($ip, $port) = $host =~ /^(.*):(\d+)$/;
    my $sock = "Sock_$host";
    my $connected = 0;
    my $proto = $PROTO_TCP ||= getprotobyname('tcp');
    my $sin;

    # try preferred ips
    if ($self->{pref_ip} && (my $prefip = $self->{pref_ip}->{$ip})) {
        _debug("using preferred ip $prefip over $ip");
        socket($sock, PF_INET, SOCK_STREAM, $proto);
        $sin = Socket::sockaddr_in($port, Socket::inet_aton($prefip));
        if (_connect_sock($sock, $sin, 0.1)) {
            $connected = 1;
        } else {
            _debug("failed connect to preferred ip $prefip");
            close $sock;
        }
    }

    # now try the original ip
    unless ($connected) {
        socket($sock, PF_INET, SOCK_STREAM, $proto);
        $sin = Socket::sockaddr_in($port, Socket::inet_aton($ip));
        return undef unless _connect_sock($sock, $sin);
    }

    # just throw back the socket we have so far
    return $sock;
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

        _fail("'timeout' argument must be a number")
            if $args{timeout} && $args{timeout} !~ /^\d+$/;
        $self->{timeout} = $args{timeout} || 3;
    }

    $self->{host_dead} = {};

    return $self;
}

# return a new mogilefsd socket, trying different hosts until one is found,
# or undef if they're all dead
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

        # try dead hosts every 5 seconds
        next if $self->{host_dead}->{$host} &&
                $self->{host_dead}->{$host} > $now - 5;

        last if $sock = $self->_sock_to_host($host);

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

sub _unescape_url_string {
    my $str = shift;
    $str =~ s/%([a-fA-F0-9][a-fA-F0-9])/pack("C", hex($1))/eg;
    $str =~ tr/+/ /;
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

*_debug = *MogileFS::_debug;

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
no strict 'refs';

use Carp;
use POSIX qw( EAGAIN );
use Socket qw( PF_INET SOCK_STREAM );
use Errno qw( EINPROGRESS EISCONN );

use vars qw($PROTO_TCP);

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
            'backup_dests',
            'bytes_out',      # count of how many bytes we've written to the socket
            'data_in',        # storage for data we've read from the socket
            );

sub path  { _getset(shift, 'path');      }
sub class { _getset(shift, 'class', @_); }
sub key   { _getset(shift, 'key', @_);   }

sub _parse_url {
    my MogileFS::NewHTTPFile $self = shift;
    my $url = shift;
    return 0 unless $url =~ m!http://(.+?)(/.+)$!;
    $self->{host} = $1;
    $self->{uri} = $2;
    $self->{path} = $url;
    return 1;
}

sub TIEHANDLE {
    my MogileFS::NewHTTPFile $self = shift;
    $self = fields::new($self) unless ref $self;

    my %args = @_;
    return undef unless $self->_parse_url($args{path});

    $self->{data} = '';
    $self->{length} = 0;
    $self->{backup_dests} = $args{backup_dests} || [];
    $self->{content_length} = $args{content_length} + 0;
    $self->{pos} = 0;
    $self->{$_} = $args{$_} foreach qw(mg fid devid class key);
    $self->{bytes_out} = 0;
    $self->{data_in} = '';

    return $self;
}
*new = *TIEHANDLE;

sub _sock_to_host { # (host)
    my MogileFS::NewHTTPFile $self = shift;
    my $host = shift;

    # setup
    my ($ip, $port) = $host =~ /^(.*):(\d+)$/;
    my $sock = "Sock_$host";
    my $proto = $PROTO_TCP ||= getprotobyname('tcp');
    my $sin;

    # create the socket
    socket($sock, PF_INET, SOCK_STREAM, $proto);
    $sin = Socket::sockaddr_in($port, Socket::inet_aton($ip));

    # unblock the socket
    IO::Handle::blocking($sock, 0);

    # attempt a connection
    my $ret = connect($sock, $sin);
    if (!$ret && $! == EINPROGRESS) {
        my $win = '';
        vec($win, fileno($sock), 1) = 1;

        # watch for writeability
        if (select(undef, $win, undef, 3) > 0) {
            $ret = connect($sock, $sin);

            # EISCONN means connected & won't re-connect, so success
            $ret = 1 if !$ret && $! == EISCONN;
        }
    }

    # just throw back the socket we have
    return $sock if $ret;
    return undef;
}

sub _connect_sock {
    my MogileFS::NewHTTPFile $self = shift;
    return 1 if $self->{sock};

    my @down_hosts;

    while (!$self->{sock} && $self->{host}) {
        # attempt to connect
        return 1 if
            $self->{sock} = $self->_sock_to_host($self->{host});

        push @down_hosts, $self->{host};
        if (my $dest = shift @{$self->{backup_dests}}) {
            # dest is [$devid,$path]
            _debug("connecting to $self->{host} (dev $self->{devid}) failed; now trying $dest->[1] (dev $dest->[0])");
            $self->_parse_url($dest->[1]) or _fail("bogus URL");
            $self->{devid} = $dest->[0];
        } else {
            $self->{host} = undef;
        }
    }

    _fail("unable to open socket to storage node (tried: @down_hosts): $!");
}

# abstracted read; implements what ends up being a blocking read but
# does it in terms of non-blocking operations.
sub _getline {
    my MogileFS::NewHTTPFile $self = shift;
    return undef unless $self->{sock};

    # short cut if we already have data read
    if ($self->{data_in} =~ s/^(.*?\r?\n)//) {
        return $1;
    }

    my $rin = '';
    vec($rin, fileno($self->{sock}), 1) = 1;

    # nope, we have to read a line
    my $nfound;
    while ($nfound = select($rin, undef, undef, 3)) {
        my $data;
        my $bytesin = sysread($self->{sock}, $data, 1024);
        if (defined $bytesin) {
            # we can also get 0 here, which means EOF.  no error, but no data.
            $self->{data_in} .= $data if $bytesin;
        } else {
            next if $! == EAGAIN;
            _fail("error reading from node for device $self->{devid}: $!");
        }

        # return a line if we got one
        if ($self->{data_in} =~ s/^(.*?\r?\n)//) {
            return $1;
        }

        # and if we got no data, it's time to return EOF
        return undef unless $bytesin;
    }

    # if we got here, nothing was readable in our time limit
    return undef;
}

# abstracted write function that uses non-blocking I/O and checking for
# writability to ensure that we don't get stuck doing a write if the
# node we're talking to goes down.  also handles logic to fall back to
# a backup node if we're on our first write and the first node is down.
# this entire function is a blocking function, it just uses intelligent
# non-blocking write functionality.
#
# this function returns success (1) or it croaks on failure.
sub _write {
    my MogileFS::NewHTTPFile $self = shift;
    return undef unless $self->{sock};

    my $win = '';
    vec($win, fileno($self->{sock}), 1) = 1;

    # setup data and counters
    my $data = shift();
    my $bytesleft = length($data);
    my $bytessent = 0;

    # main sending loop for data, will keep looping until all of the data
    # we've been asked to send is sent
    my $nfound;
    while ($bytesleft && ($nfound = select(undef, $win, undef, 3))) {
        my $bytesout = syswrite($self->{sock}, $data, $bytesleft, $bytessent);
        if (defined $bytesout) {
            # update our myriad counters
            $bytessent += $bytesout;
            $self->{bytes_out} += $bytesout;
            $bytesleft -= $bytesout;
        } else {
            # if we get EAGAIN, restart the select loop, else fail
            next if $! == EAGAIN;
            _fail("error writing to node for device $self->{devid}: $!");
        }
    }
    return 1 unless $bytesleft;

    # at this point, we had a socket error, since we have bytes left, and
    # the loop above didn't finish sending them.  if this was our first
    # write, let's try to fall back to a different host.
    unless ($self->{bytes_out}) {
        if (my $dest = shift @{$self->{backup_dests}}) {
            # dest is [$devid,$path]
            $self->_parse_url($dest->[1]) or _fail("bogus URL");
            $self->{devid} = $dest->[0];
            $self->_connect_sock;

            # now repass this write to try again
            return $self->_write($data);
        }
    }

    # total failure (croak)
    $self->{sock} = undef;
    _fail("unable to write to any allocated storage node");
}

sub PRINT {
    my MogileFS::NewHTTPFile $self = shift;

    # get data to send to server
    my $data = shift;
    my $newlen = length $data;
    $self->{pos} += $newlen;

    # now make socket if we don't have one
    if (!$self->{sock} && $self->{content_length}) {
        $self->_connect_sock;
        $self->_write("PUT $self->{uri} HTTP/1.0\r\nContent-length: $self->{content_length}\r\n\r\n");
    }

    # write some data to our socket
    if ($self->{sock}) {
        # save the first 1024 bytes of data so that we can seek back to it
        # and do some work later
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
        $self->_write($data);
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
        $self->_connect_sock;
        $self->_write("PUT $self->{uri} HTTP/1.0\r\nContent-length: $self->{length}\r\n\r\n");
        $self->_write($self->{data});
    }

    # set a message in $! and $@
    my $err = sub {
        $@ = "$_[0]\n";
        return undef;
    };

    # get response from put
    if ($self->{sock}) {
        my $line = $self->_getline;

        return $err->("Unable to read response line from server ($self->{sock})")
            unless defined $line;

        if ($line =~ m!^HTTP/\d+\.\d+\s+(\d+)!) {
            # all 2xx responses are success
            unless ($1 >= 200 && $1 <= 299) {
                # read through to the body
                my ($found_header, $body);
                while (defined (my $l = $self->_getline)) {
                    # remove trailing stuff
                    $l =~ s/[\r\n\s]+$//g;
                    $found_header = 1 unless $l;
                    next unless $found_header;

                    # add line to the body, with a space for readability
                    $body .= " $l";
                }
                $body = substr($body, 0, 512) if length $body > 512;
                return $err->("HTTP response $1 from upload: $body");
            }
        } else {
            return $err->("Response line not understood: $line");
        }
        $self->{sock}->close;
    }

    my MogileFS $mg = $self->{mg};
    my $domain = $mg->{domain};

    my $fid   = $self->{fid};
    my $devid = $self->{devid};
    my $path  = $self->{path};

    my $key = shift || $self->{key};

    my $rv = $mg->{backend}->do_request
        ("create_close", {
            fid    => $fid,
            devid  => $devid,
            domain => $domain,
            size   => $self->{content_length} ? $self->{content_length} : $self->{length},
            key    => $key,
            path   => $path,
        });
    unless ($rv) {
        # set $@, as our callers expect $@ to contain the error message that
        # failed during a close.  since we failed in the backend, we have to
        # do this manually.
        return $err->("$mg->{backend}->{lasterr}: $mg->{backend}->{lasterrstr}");
    }

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

*_debug = *MogileFS::_debug;

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

sub _fid {
    my MogileFS::NewHTTPFile $self = shift;
    return $self->{fid};
}

1;
