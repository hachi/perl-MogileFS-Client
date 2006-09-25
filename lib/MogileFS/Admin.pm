package MogileFS::Admin;
use strict;
use Carp;
use MogileFS::Backend;
use fields qw(backend readonly);

sub new {
    my MogileFS::Admin $self = shift;
    $self = fields::new($self) unless ref $self;

    my %args = @_;

    $self->{readonly} = $args{readonly} ? 1 : 0;
    $self->{backend} = MogileFS::Backend->new( hosts => $args{hosts} )
        or _fail("couldn't instantiate MogileFS::Backend");

    return $self;
}

sub readonly {
    my MogileFS::Admin $self = shift;
    return $self->{readonly} = $_[0] ? 1 : 0 if @_;
    return $self->{readonly};
}

sub replicate_now {
    my MogileFS::Admin $self = shift;

    my $res = $self->{backend}->do_request("replicate_now", {})
        or return undef;
    return 1;
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

1;
