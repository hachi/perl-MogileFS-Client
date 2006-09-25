######################################################################
# MogileFS::NewDiskFile object
#

package MogileFS::NewDiskFile;

use strict;
use IO::File;
use base 'IO::File';
use Carp;

sub new {
    my MogileFS::NewDiskFile $self = shift;
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
    my MogileFS::NewDiskFile $self = shift;

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
# MogileFS::NewDiskFile class methods
#

sub _fail {
    croak "MogileFS::NewDiskFile: $_[0]";
}

*_debug = *MogileFS::_debug;

# get a reference to the hash part of the $fh typeglob ref
sub _get_attrs {
    return \%{*{$_[0]}};
}

sub _getset {
    my MogileFS::NewDiskFile $self = shift;
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
