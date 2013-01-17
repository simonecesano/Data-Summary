use strict;
no warnings;
package Data::Summary;

no warnings qw/numeric uninitialized/;

use Moose;
use Carp;
use Data::Dump qw/dump/;
use List::Util qw/sum/;
use List::MoreUtils qw/uniq/;
use Memoize;

my $DEBUG = 1;

$\ = "\n";

memoize('data');
# memoize('summary');

has group_by => ( is => 'rw', isa => 'ArrayRef' );

has summarize => ( is => 'rw', isa => 'ArrayRef' );

# summarize takes an arrayref of arrarefs like this:

#     summarize => [
# 		  [ 0  => sub { (scalar uniq @_) || "" }, "id count" ],
# 		  [ 17 => sub { sprintf ("%.0f", (sum @_) / 100000) || "" }, "total qty" ],
# 		  [ _ => "sub { _0 > 0 ? sprintf ('%.0f', (_1 / _0)) : '' }", "average qty" ],
# 		 ]
    
# the array contains:

# in the first position:
# - the field number for the data that gets summarized
# - or an underscore

# in the second position
# - a subref for the summary function
# - or a string that evaluates to a subref

# and in the third position the name of the summarized field

# The subref in the second position will take an array containing the elements in the N-th field in the input data, where N is the number in the first position.   
# So  [ 0  => sub { (scalar uniq @_) || "" ] means that the summary will be given by the count of unique elements on the 0-th field in the input data

# If the second position contains a string, the following will get substituted:

# - an underscore followed by a number gets substituted with the result of the respective summary function (by position)
# - a hash sign ('#') followed by a number gets substituted with the result of the respective summary function (by position) forced to a number
# - a percent sign followed by a number gets substituted with the list of the N-th field in the input data  

# So these are the same:

#       [ 0 => sub { (scalar uniq @_) || "" } ],
#       [ _ => "sub { (scalar uniq %0) || '' }" ],
 

has summary_func => ( is => 'rw', isa => 'ArrayRef' );

# pivot contains the columns headers
has pivot => ( is  => 'rw', isa => 'ArrayRef' );

# this contains the map between the field and the summary position
has summary_map => (
	      is => 'rw',
	      isa => 'HashRef',
	      default   => sub { {} },
);

# this is the place where all the data gets stored
has accum => (
	      is => 'rw',
	      traits    => ['Hash'],
	      isa => 'HashRef',
	      default   => sub { {} },
	     );

has cols => (
	      is => 'rw',
	      traits    => ['Hash'],
	      isa => 'HashRef',
	      default   => sub { {} },
	     );

has rows => (
	      is => 'rw',
	      traits    => ['Hash'],
	      isa => 'HashRef',
	      default   => sub { {} },
	     );

has keys => (
	     is => 'rw',
	     traits    => ['Hash'],
	     isa => 'HashRef',
	     default   => sub { {} },
	    );

#----------------------------------------------------------------
# these two are the functions used to store data
#----------------------------------------------------------------
has _add => ( is => 'rw' );
has _col => ( is => 'rw' );
has _row => ( is => 'rw' );
has _key => ( is => 'rw' );

sub BUILD {
    my $self = shift;

    {
	#----------------------------------------------------------------
	# build a function string that looks like this:
	# sub { 
	#     my $self = shift;
	#     my $val = [ @_[ 0 ] ];
	#     push @{ $self->accum->{$_[12]}->{$_[1]}->{$_[2]}->{$_[3]}->{$_[4]} }, $val;
	#     return $self->accum->{$_[12]}->{$_[1]}->{$_[2]}->{$_[3]}->{$_[4]}
	# }
	#----------------------------------------------------------------

	my $rstore = join '', '$self->accum', map { "->{\$_[$_]}" } (@{$self->group_by}, @{$self->pivot});
	my $val = join ', ', $self->summary_fields;
	$rstore = sprintf 'sub { my $self = shift; my $val = [ @_[ %s ] ]; push @{ %s }, $val; return %s }', $val, $rstore, $rstore ;

	$self->_add(eval $rstore);

	#----------------------------------------------------------------
	# build a function that looks like this:
	# sub { my $self = shift; $self->cols->{$_[4]}++ }
	#----------------------------------------------------------------

	my $cstore = join '', '$self->cols', map { "->{\$_[$_]}" } (@{$self->pivot});
	$cstore = sprintf 'sub { my $self = shift; %s++ }', $cstore;
	$self->_col(eval $cstore);

	my $rstore = join '', '$self->rows', map { "->{\$_[$_]}" } (@{$self->group_by});
	$rstore = sprintf 'sub { my $self = shift; %s++ }', $rstore;
	$self->_row(eval $rstore);

	my $kstore = join ', ',  (@{$self->group_by}, @{$self->pivot});
	$kstore = sprintf 'sub { my ($self, @val) = @_; for (%s) { $self->keys->{$_}->{$val[$_]}++ } }', $kstore;
	print STDERR $kstore, eval $kstore if $DEBUG;
	$self->_key(eval $kstore)
    };

    {
	my $i;
	$self->summary_map({ map { $_ => $i++} sort { $a <=> $b } uniq grep { /^\d+$/ } map { $_->[0] } (@{$self->summarize}) });
	dump $self->summary_map if $DEBUG;
    };

    {
	for (@{$self->summarize}) {
	    next if ref $_->[1] eq 'CODE';
	    $_->[1] = $self->_string_to_summary_code($_->[1]);
	    $_->[0] = '*'
	}
    };
}
sub add_data {
    my $self = shift;
    my $data = shift;

    $self->_add->($self, @$data);
    $self->_col->($self, @$data);
    $self->_row->($self, @$data);
    $self->_key->($self, @$data);
}

sub summary_fields {
    my $self = shift;
    return sort { $a <=> $b } uniq grep { /^\d+$/ } map { $_->[0] } (@{$self->summarize}) 
}

sub _keys {
    #--------------------------------------------------------------------------------------------
    # messing with this gives a subtotals first or subtotals at the end result
    #--------------------------------------------------------------------------------------------
    my $d = shift;
    my ($min, $max, $keys, $accum) = @_;
    if ((ref $d eq 'HASH') && 1) {
	for (sort keys %$d) {
	    my $keys = [ @$keys ];
	    push @$keys, $_;
	    push @{$accum}, [ @$keys[$min..$#$keys] ]
		if (scalar @$keys <= $max) && (scalar @$keys > $min);
	    _keys($d->{$_}, $min, $max, $keys, $accum);
	}
    }
}

sub row_headers {
    my $self = shift;
    my $all = shift;
    my $k = []; $a = [];
    _keys($self->accum, 0, (scalar @{$self->group_by}), $k, $a);

    return @$a;
}

sub col_headers {
    my $self = shift;
    my $rkey = shift;

    my $k = []; $a = [];
    _keys($self->accum, (scalar @{$self->group_by}), (scalar @{$self->group_by}) + (scalar @{$self->pivot}), $k, $a);

    my $pivot = scalar @{ $self->pivot } - 1;
    my $sort = $self->make_sort(0, $pivot);

    my @headers;
    #my @c = sort { $sort->($a, $b) } $self->col_headers;



    if ((defined $rkey) && (scalar @$rkey)) { 
	@headers = sort { $sort->($a, $b) } map { [ _unpack_key($_) ] } keys %{ $self->accum->{_pack_key(@$rkey)} };
    } else {
	@headers = sort { $sort->($a, $b) } map { [ _unpack_key($_) ] } uniq map { _pack_key(@$_) } @$a;
    }
    return @headers;
}


sub make_sort { 
    no warnings qw/uninitialized/;
    my $self = shift;
    my @s = @_;
    my $s = 'sub { ' . (join ' || ', map { sprintf '$_[0]->[%d] cmp $_[1]->[%d]', $_, $_ } @s) . ' || (scalar @$_[1] <=> scalar @$_[0]) }';
    return eval $s
}

sub summary {
    my $self = shift;
    my ($rkey, $ckey, $seq) = @_;
    if (defined $seq) {
	return $self->_summary($rkey, $ckey, $seq)
    } else {
	return map { $self->_summary($rkey, $ckey, $_) } (0..$#{$self->summarize})
    }
}

sub _string_to_summary_code {
    my $self = shift;
    my $sub = shift;
    my $string = $sub;
    # substitute % followed by a number with a map to that data field

    print STDERR $sub if $DEBUG;

    for ($sub) {
	!/^\s*sub/ && do {
	    $_ = join '', 'sub { ', $_, ' }';
	};
	/%\d/ && do {
	    s/sub {/sub { my (\$self, \$rkey, \$ckey) = \@_; no warnings qw\/uninitialized numeric\/;/;
	    s/%(\d+)/map { \$_->[$1] } \@{\$self->data(\$rkey, \$ckey)}/g;
	};
	/#\d/ && do {
	    s/sub {/sub { my (\$self, \$rkey, \$ckey) = \@_; no warnings qw\/uninitialized numeric\/;/;
	    my @m = /#(\d+)/g;
	    for my $m (@m) {
		my $r = "\$self->_summary(\$rkey, \$ckey, $m) + 0";
		s/#$m/$r/g;
	    }
	};
	/_\d/ && do {
	    s/sub {/sub { my (\$self, \$rkey, \$ckey) = \@_; no warnings qw\/uninitialized numeric\/;/;
	    my @m = /_(\d+)/g;
	    for my $m (@m) {
		my $r = "\$self->_summary(\$rkey, \$ckey, $m)";
		s/_$m/$r/g;
	    }
	};
    }
    print STDERR $sub if $DEBUG;

    return eval $sub || croak "string\n\n\t$string\n\ntransformed to\n\n\t$sub\n\ndoes not evaluate to code";
}

use B::Deparse;
my $deparse = B::Deparse->new("-p", "-sC");
use Data::Dump qw/dump/;

sub _summary {
    my $self = shift;
    my ($rkey, $ckey, $seq) = @_;
    # watch it! this isn't the field - it's just the position in summarize!
    # maybe this is a bad idea - why not use the actual thing

    no warnings qw/uninitialized numeric/;

    croak "there is no summary function at place $seq" if $seq > $#{$self->summarize->[$seq]};

    my ($field, $func) = @{$self->summarize->[$seq]};

    if ($field eq '_') {
	my @data = @{$self->data($rkey, $ckey)};
	return $func->(@data);
    } elsif ($field eq '*') {
 	return $func->($self, $rkey, $ckey)
    } else {
	my $pos = $self->summary_map->{$field};
	my @data = map { $_->[$pos] } @{$self->data($rkey, $ckey)};
	if (ref $func eq "CODE") {
	    return $func->(@data)
	} else {
	    $func = eval join '', 'sub { ', $func, '(@_) }';
	    return $func->(@data);
	}
    }
}

sub data {
    my $self = shift;
    
    my @rkey = @{ $_[0] }; 
    $#rkey = $#{$self->group_by};
    
    my @ckey = @{ $_[1] };
    $#ckey = $#{$self->pivot};

    my $a = [];
    return _find($self->accum, $a, @rkey, @ckey);
}

sub _find {
    my ($r, $a, @path) = @_;
    if (ref $r eq "ARRAY") {
	push @$a, @{$r};
    } elsif (defined $path[0]) {
	_find($r->{shift @path}, $a, @path);
    } else {
	shift @path;
	for (keys %{$r}) {
	    _find($r->{$_}, $a, @path);
	}
    }
    return $a;
}

sub _pack_key {
    return pack 'N' . ('w/a*' x @_), scalar(@_), @_;
}

sub _unpack_key {
    return unpack 'x4' . ('w/a*' x (unpack 'N', $_[0])), $_[0];
}

__PACKAGE__->meta->make_immutable();

1;
