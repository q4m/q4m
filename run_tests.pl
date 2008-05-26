#! /usr/bin/perl

use strict;
use warnings;

use Test::Harness;

runtests(@ARGV ? @ARGV : <t/*.t>);
