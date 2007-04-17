#!/usr/bin/perl
package MogileFS::Client;

use strict;
use Carp;
use IO::WrapTie;
use LWP::UserAgent;
use fields ('root',      # filesystem root.  only needed for now-deprecated NFS mode.  don't use.
            'domain',    # scalar: the MogileFS domain (namespace).
            'backend',   # MogileFS::Backend object
            'readonly',  # bool: if set, client won't permit write actions/etc.  just reads.
            'hooks',     # hash: hookname -> coderef
            );
use Time::HiRes ();
use MogileFS::Backend;
use MogileFS::NewHTTPFile;

our $AUTOLOAD;

sub new {
    my MogileFS::Client $self = shift;
    $self = fields::new($self) unless ref $self;

    return $self->_init(@_);
}

sub reload {
    my MogileFS::Client $self = shift;
    return undef unless $self;

    return $self->_init(@_);
}

sub _init {
    my MogileFS::Client $self = shift;

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

sub run_hook {
    my MogileFS::Client $self = shift;
    my $hookname = shift || return;

    my $hook = $self->{hooks}->{$hookname};
    return unless $hook;

    eval { $hook->(@_) };

    warn "MogileFS::Client hook '$hookname' threw error: $@\n" if $@;
}

sub add_hook {
    my MogileFS::Client $self = shift;
    my $hookname = shift || return;

    if (@_) {
        $self->{hooks}->{$hookname} = shift;
    } else {
        delete $self->{hooks}->{$hookname};
    }
}

sub add_backend_hook {
    my MogileFS::Client $self = shift;
    my $backend = $self->{backend};

    $backend->add_hook(@_);
}

sub last_tracker {
    my MogileFS::Client $self = shift;
    return $self->{backend}->last_tracker;
}

sub errstr {
    my MogileFS::Client $self = shift;
    return $self->{backend}->errstr;
}

sub errcode {
    my MogileFS::Client $self = shift;
    return $self->{backend}->errcode;
}

sub readonly {
    my MogileFS::Client $self = shift;
    return $self->{readonly} = $_[0] ? 1 : 0 if @_;
    return $self->{readonly};
}

# expects as argument a hashref of "standard-ip" => "preferred-ip"
sub set_pref_ip {
    my MogileFS::Client $self = shift;
    $self->{backend}->set_pref_ip(shift)
        if $self->{backend};
}

# returns MogileFS::NewHTTPFile object, or undef if no device
# available for writing
# ARGS: ( key, class, bytes?, opts? )
# where bytes is optional and the length of the file and opts is also optional
# and is a hashref of options.  supported options: fid = unique file id to use
# instead of just picking one in the database.
sub new_file {
    my MogileFS::Client $self = shift;
    return undef if $self->{readonly};

    my ($key, $class, $bytes, $opts) = @_;
    $bytes += 0;
    $opts ||= {};

    # Extra args to be passed along with the create_open and create_close commands.
    # Any internally generated args of the same name will overwrite supplied ones in
    # these hashes.
    my $create_open_args =  $opts->{create_open_args} || {};
    my $create_close_args = $opts->{create_close_args} || {};

    $self->run_hook('new_file_start', $self, $key, $class, $opts);

    my $res = $self->{backend}->do_request
        ("create_open", {
            %$create_open_args,
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

    # create a MogileFS::NewHTTPFile object, based off of IO::File
    unless ($main_path =~ m!^http://!) {
        Carp::croak("This version of MogileFS::Client no longer supports non-http storage URLs.\n");
    }

    $self->run_hook('new_file_end', $self, $key, $class, $opts);

    return IO::WrapTie::wraptie('MogileFS::NewHTTPFile',
                                mg    => $self,
                                fid   => $res->{fid},
                                path  => $main_path,
                                devid => $main_devid,
                                backup_dests => $dests,
                                class => $class,
                                key   => $key,
                                content_length => $bytes+0,
                                create_close_args => $create_close_args,
                                );
}

# Wrapper around new_file, print, and close.
# Given a key, class, and a filehandle or filename, stores the
# file contents in MogileFS. Returns the number of bytes stored on
# success, undef on failure.
sub store_file {
    my MogileFS::Client $self = shift;
    return undef if $self->{readonly};

    my($key, $class, $file, $opts) = @_;
    $self->run_hook('store_file_start', $self, $key, $class, $opts);

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

    $self->run_hook('store_file_end', $self, $key, $class, $opts);

    close $fh_from unless ref $file;
    $fh->close or return;
    $bytes;
}

# Wrapper around new_file, print, and close.
# Given a key, class, and file contents (scalar or scalarref), stores the
# file contents in MogileFS. Returns the number of bytes stored on
# success, undef on failure.
sub store_content {
    my MogileFS::Client $self = shift;
    return undef if $self->{readonly};

    my($key, $class, $content, $opts) = @_;

    $self->run_hook('store_content_start', $self, $key, $class, $opts);

    my $fh = $self->new_file($key, $class, undef, $opts) or return;
    $content = ref($content) eq 'SCALAR' ? $$content : $content;
    $fh->print($content);

    $self->run_hook('store_content_end', $self, $key, $class, $opts);

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
    my MogileFS::Client $self = shift;
    my ($key, $opts) = @_;

    # handle parameters, if any
    my ($noverify, $zone);
    if (ref $opts) {
        $noverify = 1 if $opts->{noverify};
        $zone = $opts->{zone} || undef;
    } else {
        $noverify = 1 if $opts;
    }

    $self->run_hook('get_paths_start', $self, $key, $opts);

    my $res = $self->{backend}->do_request
        ("get_paths", {
            domain => $self->{domain},
            key    => $key,
            noverify => $noverify ? 1 : 0,
            zone   => $zone,
        }) or return ();

    my @paths = map { $res->{"path$_"} } (1..$res->{paths});

    $self->run_hook('get_paths_end', $self, $key, $opts);

    return @paths if scalar(@paths) > 0 && $paths[0] =~ m!^http://!;
    return map { "$self->{root}/$_"} @paths;
}

# given a key, returns a scalar reference pointing at a string containing
# the contents of the file. takes one parameter; a scalar key to get the
# data for the file.
sub get_file_data {
    # given a key, load some paths and get data
    my MogileFS::Client $self = $_[0];
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

# this method returns undef only on a fatal error such as inability to actually
# delete a resource and inability to contact the server.  attempting to delete
# something that doesn't exist counts as success, as it doesn't exist.
sub delete {
    my MogileFS::Client $self = shift;
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
    my MogileFS::Client $self = shift;
    my $duration = shift;

    $self->{backend}->do_request("sleep", { duration => $duration + 0 })
        or return undef;

    return 1;
}

# this method renames a file.  it returns an undef on error (only a fatal error
# is considered as undef; "file didn't exist" isn't an error).
sub rename {
    my MogileFS::Client $self = shift;
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
    my MogileFS::Client $self = shift;
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

sub foreach_key {
    my MogileFS::Client $self = shift;
    my $callback = pop;
    Carp::croak("Last parameter not a subref") unless ref $callback eq "CODE";
    my %opts = @_;
    my $prefix = delete $opts{prefix};
    Carp::croak("Unknown option(s): " . join(", ", keys %opts)) if %opts;

    my $last = "";
    my $max = 1000;
    my $count = $max;

    while ($count == $max) {
        my $res = $self->{backend}->do_request
            ("list_keys", {
                domain => $self->{domain},
                prefix => $prefix,
                after => $last,
                limit => $max,
            }) or return undef;
        $count = $res->{key_count}+0;
        for (my $i = 1; $i <= $count; $i++) {
            $callback->($res->{"key_$i"});
        }
        $last = $res->{"key_$count"};
    }
    return 1;
}

# used to support plugins that have modified the server, this builds things into
# an argument list and passes them back to the server
# TODO: there is no readonly protection here?  does it matter?  should we check
# with the server to see what methods they support?  (and if they should be disallowed
# when the client is in readonly mode?)
sub AUTOLOAD {
    # remove everything up to the last colon, so we only have the method name left
    my $method = $AUTOLOAD;
    $method =~ s/^.*://;

    return if $method eq 'DESTROY';

    # let this work
    no strict 'refs';

    # create a method to pass this on back
    *{$AUTOLOAD} = sub {
        my MogileFS::Client $self = shift;
        # pre-assemble the arguments into a hashref
        my $ct = 0;
        my $args = {};
        $args->{"arg" . ++$ct} = shift() while @_;
        $args->{"argcount"} = $ct;

        # now put in standard args
        $args->{"domain"} = $self->{domain};

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


1;
__END__

=head1 NAME

MogileFS::Client - client library for the MogileFS distributed file system

=head1 SYNOPSIS

 use MogileFS::Client;

 # create client object w/ server-configured namespace and IPs of trackers
 $mogc = MogileFS::Client->new(domain => "foo.com::my_namespace",
                               hosts  => ['10.0.0.2', '10.0.0.3']);

 # create a file
 $key   = "image_of_userid:$userid";   # mogile is a flat namespace.  no paths.
 $class = "user_images";               # must be configured on server
 $fh = $mogc->new_file($key, $class);

 print $fh $data;

 unless ($fh->close) {
    die "Error writing file: " . $mogc->errcode . ": " . $mogc->errstr;
 }

 # Find the URLs that the file was replicated to.  May change over time.
 @urls = $mogc->get_paths($key);

 # no longer want it?
 $mogc->delete($key);

 # read source for more methods.  those are the major ones.

=head1 DESCRIPTION

See http://www.danga.com/mogilefs/

=head1 COPYRIGHT

This module is Copyright 2003-2004 Brad Fitzpatrick,
and copyright 2005-2006 Six Apart, Ltd.

All rights reserved.

You may distribute under the terms of either the GNU General Public
License or the Artistic License, as specified in the Perl README file.

=head1 WARRANTY

This is free software. IT COMES WITHOUT WARRANTY OF ANY KIND.

=head1 AUTHORS

Brad Fitzpatrick <brad@danga.com>

Brad Whitaker <whitaker@danga.com>

Mark Smith <marksmith@danga.com>

