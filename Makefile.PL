#!/usr/bin/perl
#
#   Perl Makefile for Danga-Socket
#   $Id$
#
#   Invoke with 'perl Makefile.PL'
#
#   See ExtUtils::MakeMaker (3) for more information on how to influence
#    the contents of the Makefile that is written
#

use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                    => 'MogileFS',
    VERSION                 => '1.00',
    AUTHOR                  => 'Brad Whitaker <whitaker@danga.com>',
    ABSTRACT                => 'MogileFS client library',
    PREREQ_PM               => {
        'IO::WrapTie'     => '2.102',
        'LWP::Simple'     => 0,
        fields            => 0,
    },
    dist                    => {
        CI                      => "cvs commit",
        RCS_LABEL               => 'cvs tag RELEASE_$(VERSION_SYM)',
        SUFFIX                  => ".gz",
        DIST_DEFAULT            => 'all tardist',
        COMPRESS                => "gzip",
    },

);

