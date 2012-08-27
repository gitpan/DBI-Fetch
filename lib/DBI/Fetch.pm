package DBI::Fetch;

# Copyright (c) 2012 Iain Campbell. All rights reserved.
#
# This work may be used and modified freely, but I ask that the copyright 
# notice remain attached to the file. You may modify this module as you 
# wish, but if you redistribute a modified version, please attach a note 
# listing the modifications you have made.

BEGIN {
    $DBI::Fetch::AUTHORITY = 'cpan:CPANIC';
    $DBI::Fetch::VERSION   = '1.00';
    $DBI::Fetch::VERSION   = eval $DBI::Fetch::VERSION;
}

use 5.008_004;
use strict;
use warnings::register;
use warnings 'all';
use base 'Exporter';

use DBI;

use Params::Callbacks qw/
    callbacks 
    /;

use Scalar::Util qw/
    blessed
    reftype
   /;

our @EXPORT;

our @EXPORT_OK = qw/
    process
    /;

our %EXPORT_TAGS = (
    default => \@EXPORT,
    all     => \@EXPORT_OK,
);

our $E_UNIMP_METHOD = 'Method "%s" not implemented by package "%s"';
our $E_NO_DBH       = 'Database handle expected';
our $E_NO_SQL       = 'SQL statement expected';
our $E_EXP_L_REF    = 'Hash or array reference expected';
our $E_EXP_A_REF    = 'Array reference expected';
our $E_EXP_H_REF    = 'Hash reference expected';
our $E_EXP_STH      = 'Statement handle expected but got %s object';

sub is_array_ref { ref $_[0] && reftype $_[0] eq 'ARRAY' }

sub is_hash_ref  { ref $_[0] && reftype $_[0] eq 'HASH' }

sub is_code_ref  { ref $_[0] && reftype $_[0] eq 'CODE' }

our @CONFIG = ( { 
    remember_last_used_dbh    => 1,
    return_result_sets_as_ref => 0,
    fetch_row_using           => sub { 
        $_[0]->fetchrow_hashref('NAME_lc') 
    },
} );

sub throw {
    @_ = $@ = ref $_[0] ? ref $_[0] : sprintf @_ ? shift : $@, @_ ;
    defined &Carp::croak ? goto &Carp::croak : die;
};

sub config {
    shift if @_ && __PACKAGE__ eq "$_[0]";
    
   if (@_) {
        if (is_hash_ref($_[0])) {
            $CONFIG[-1] = shift;    
        }
        else {
            my %config = @_;
          
            while (my ($key, $value) = each %config) {
                $CONFIG[-1]{$key} = $value;

                if ($key eq 'dbh' && !exists($config{remember_last_used_dbh})) {
                    $config{remember_last_used_dbh} = $CONFIG[-1]{$key} ? 0 : 1;                 
                }
            }
        }
    }

    return $CONFIG[-1];
}

sub push_config {
    shift if @_ && __PACKAGE__ eq "$_[0]";
    push @CONFIG, { %{ $CONFIG[-1] } };
    @_ ? &config : $CONFIG[-1];
}

sub pop_config {
    shift if @_ && __PACKAGE__ eq "$_[0]";
    pop @CONFIG if @CONFIG > 1;
    @_ ? &config : $CONFIG[-1];
}

# Determine number and style of placeholders used in the SQL statement. A
# hash-reference containing "style" and "count" elements is returned. If
# no placeholders were found then undef is returned.

sub placeholder_disposition {
    shift if @_ && __PACKAGE__ eq "$_[0]";
    
    my $sql = shift 
        or return;
    
    my $count = 0;
    my $style = undef;
    
    $count += 1 while $sql =~ m{:\d+\b}gso;
    
    if ($count) { 
        $style = ':1'; 
    } 
    else {
        $count += 1 while $sql =~ m{:\w+\b}gso;
        
        if ($count) { 
            $style = ':name'; 
        }
        else {
            $count += 1 while $sql =~ m{\?}gso;
            $style = '?' if $count;
        }
    }

    wantarray ? ( style => $style, count => $count ) : $style; 
}

# If the statement contains :1-style or :name-style placeholders then they 
# will be converted to the standard ?-style placeholders and parameters
# are ordered accordingly.

sub normalize {
    shift if @_ && __PACKAGE__ eq "$_[0]";
    
    my $sql = shift 
        or return;

    my $style = placeholder_disposition($sql) 
        or return $sql;
    
    my $params = do {
        my $argc = @_;
        
        if ($style eq ':name') {
            if ($argc == 1 && is_hash_ref($_[0])) {
                +{ %{ $_[0] } };
            }
            elsif ($argc == 1 && is_array_ref($_[0])) {
                +{ @{ $_[0] } };
            }
            else {
                +{ @_ };
            }
        }
        elsif ($style eq ':1') {
            my $position = 0;
            
            if ($argc == 1 && is_hash_ref($_[0])) {
                +{ %{ $_[0] } };
            }
            elsif ($argc == 1 && is_array_ref($_[0])) {
                +{ map { ( ':' . ++$position => $_ ) } @{ $_[0] } };
            }
            else {
                +{ map { ( ':' . ++$position => $_ ) } @_ };
            }
        }
        else {
            if ($argc == 1 && is_array_ref($_[0])) {
                [ @{ $_[0] } ];
            }
            else {
                [ @_ ];
            }
        }
    };

    if (is_hash_ref($params)) {
        for my $k (keys %{$params}) {
            unless (substr($k, 0, 1) eq ':') {
                $params->{':' . $k} = delete $params->{$k};
            }
        }

        my @ph_names;

        while ($sql =~ m{(:\w+)\b}gso) {
            push @ph_names, $1;
        }

        for my $name (@ph_names) {
            my $value = $params->{$name};

            if (is_array_ref($value)) {
                unless ($#{$value} == 1 && is_hash_ref($value->[1])) {
                    my $replacement = join ', ', map { '?' } @{$value};
                    s{$name\b}{$replacement}gs for $sql;
                    next;
                }
            }

            s{$name\b}{?}gs for $sql;
        }

        $params = [ map { 
            my $value = $params->{$_}; 
            if (is_array_ref($value)) {
                $#{$value} == 1 && is_hash_ref($value->[1]) ? $value : @{$value};
            }
            else {
                $value;
            }
        } @ph_names ];
    }

    return $sql, @{$params};
}

# Prepare (if necessary), bind parameters to and execute the SQL statement
# applying any optional callbacks to the result. The result is returned as
# reference. 

sub process {
    shift if @_ && __PACKAGE__ eq "$_[0]";
    
    my ($callbacks, @args) = &callbacks;

    my $config = $CONFIG[-1];

    my $dbh = do {
        if (is_code_ref($args[0])) {
            $args[0] = $args[0]->();
        }

        if (my $class = blessed $args[0]) {
            if ($class->can('prepare')) {
                shift @args;
            }            
        } 
    };

    if ($dbh) {
        $config->{dbh} = $dbh if $config->{remember_last_used_dbh};
    }
    else {
        $dbh = $config->{dbh};
    }

    throw $E_NO_DBH unless $dbh;

    my $sth = do {
        if (is_code_ref($args[0])) {
            $args[0] = $args[0]->();
        }
    
        if (my $class = blessed $args[0]) {
            if ($class->can('execute')) {
                shift @args;
            }            
            else {
                throw $E_EXP_STH, $class;
            }    
        }
        else {
            undef;
        } 
    };

    if ($sth) { # Statement was already prepared...
        if ($sth->{NUM_OF_PARAMS}) {
            my $sql = $sth->{Statement};
            my $style = placeholder_disposition($sql);

            if ($style eq ':name') {
                my %params = @args;
                
                while ($sql =~ m{:(\w+)\b}gso) {
                    my $name = $1;
                    my $value = exists $params{$name} 
                        ? $params{$name}    : exists $params{":$name"} 
                        ? $params{":$name"} : undef;
                    if (is_array_ref($value)) {
                        if ($#{$value} == 1 && is_hash_ref($value->[1])) {
                            $sth->bind_param(":$name", @{$value});
                        }
                    }
                    else {
                        $sth->bind_param(":$name", $value);
                    }
                }
            }
            elsif ($style eq ':1') {
                for my $position (1 .. @args) {
                    my $arg = $args[ $position - 1 ];
                    if (is_array_ref($arg)) {
                        if ($#{$arg} == 1 && is_hash_ref($arg->[1])) {
                            $sth->bind_param(":$position", @{$arg});
                        }
                    }
                    else {
                        $sth->bind_param(":$position", $arg);
                    }
                }
            }
            else {
                for my $position (1 .. @args) {
                    my $arg = $args[ $position - 1 ];
                    if (is_array_ref($arg)) {
                        if ($#{$arg} == 1 && is_hash_ref($arg->[1])) {
                            $sth->bind_param($position, @{$arg});
                        }
                    }
                    else {
                        $sth->bind_param($position, $arg);
                    }
                }
            }
        }
    }
    else { # Statement was not prepared...
        my ($sql, @params) = normalize(@args);

        throw $E_NO_SQL unless $sql;

        eval { $sth = $dbh->prepare($sql) };

        throw if $@;

        if ($sth->{NUM_OF_PARAMS}) {
            for my $position (1 .. @params) {
                my $arg = $params[ $position - 1 ];
                if (is_array_ref($arg)) {
                    if ($#{$arg} == 1 && is_hash_ref($arg->[1])) {
                        $sth->bind_param($position, @{$arg});
                    }
                }
                else {
                    $sth->bind_param($position, $arg);
                }
            }
        }
    }    

    eval { $sth->execute };

    throw if $@;

    my @results;

    if ($sth->{NUM_OF_FIELDS}) {
        while (my $row = $config->{fetch_row_using}->($sth)) {
            push @results, $callbacks->yield($row);
        }
    
        $sth->finish;

        if ($config->{return_result_sets_as_ref}) {
            if ($config->{auto_pop_config}) {
                @CONFIG > 1 
                    ? pop_config() 
                    : delete $config->{auto_pop_config};
            }

            return \@results; 
        }
    }
    else {
        @results = $callbacks->yield($sth->rows);
        
        $sth->finish;

        if ($config->{auto_pop_config}) {
            @CONFIG > 1 
                ? pop_config() 
                : delete $config->{auto_pop_config};
        }
    }

    return wantarray
        ? @results 
        : @results != 1 ? @results : $results[0];
}

1;

__END__

=pod

=encoding utf-8

=head1 NAME

DBI::Fetch - Prepare SQL statements, execute them and process the results easily.

=head1 SYNOPSIS

    use DBI::Fetch qw/process/;

    my $dbh = DBI->connect("dbi:mysql:database=test;host=localhost", "testuser", "testpass", { 
        RaiseError => 1, 
        PrintError => 0,
    } );

    # Execute statements, or just continue using $dbh->do(...)
    #
    process $dbh, << "__EOF__";
        CREATE TABLE tracks (
            id INT,
            name VARCHAR(20)
        )
    __EOF__

    process $dbh, << "__EOF__";
        INSERT INTO tracks 
            (id, name) 
        VALUES 
            (1, 'The Wings of Icarus'),
            (2, 'Pulsar'),
            (3, 'The Sentinel'),
            (4, 'Adrift on Celestial Seas')
    __EOF__

    # Get a single row
    #
    my $result = process $dbh, "SELECT * FROM tracks WHERE id = 2";

    # Get multiple rows
    #
    my @result = process $dbh, "SELECT * FROM tracks";
    
    # Get all result sets using lower-overhead return as reference
    #
    DBI::Fetch->config(return_result_sets_as_ref => 1);
    
    my $result = process $dbh, "SELECT * FROM tracks";
    
    # Get a result set using lower-overhead return as reference
    # for the next query only
    #
    DBI::Fetch->push_config(return_result_sets_as_ref => 1);
    
    my $result = process $dbh, "SELECT * FROM tracks";
    
    DBI::Fetch->pop_config;

    # Get result set using lower-overhead return as reference
    # for the next query only
    #
    DBI::Fetch->push_config(
        return_result_sets_as_ref => 1, 
        auto_pop_config           => 1
    );
    
    my $result = process $dbh, "SELECT * FROM tracks";

    # Process rows immediately using fetch-time callbacks
    #
    my @names = process $dbh, "SELECT name FROM tracks", sub {
        my $row = shift;
        return $row->{name};
    };

    # Provide parameterized input using ANY of the placeholder styles
    # popularised by some of the drivers.
    #
    my $id = 3;
    
    # Using standard "?" placeholder notation
    #
    my $result = process( 
        $dbh, "SELECT * FROM tracks WHERE id = ?", $id
    );

    # Or the Pg ":1" placeholder notation
    #
    my $result = process(
        $dbh, "SELECT * FROM tracks WHERE id = :1", $id
    );

    # Or the Oracle ":name" placeholder notation
    #
    my $result = process(
        $dbh, "SELECT * FROM tracks WHERE id = :id", id => $id
    );

    # Same again using ":" prefix on parameter keys for clarity
    #
    my $result = process(
        $dbh, "SELECT * FROM tracks WHERE id = :id", ":id" => $id
    );

    print "Track #$id $result\n";

    # Mixing parameterized input and fetch-time callbacks is easy
    #
    process $dbh, "SELECT * FROM tracks WHERE id = :id", id => $id, sub {
        my $row = shift;
        print "Track #$row->{id} $row->{name}\n";    
    };

=head1 DESCRIPTION

I had three goals when creating the C<DBI::Fetch> module:

=over 4

=item 1

Help developers who need to interact with DBI directly do so but with
less code that does much more.

=item 2

Remove the irritation of having to adjust to a different parameter placeholder 
style at the least convenient time.

=item 3 

Provide a simple and more intuitive method for processing result sets.

=back

=head1 PROCESSING RESULTS

=over 4

=item RESULT = B<DBI::Fetch-E<gt>process(> [I<DB-HANDLE>,] I<STATEMENT> [, I<PARAMETER-LIST>] [, I<CALLBACK-LIST>] B<);>

=item RESULT = B<DBI::Fetch::process(> [I<DB-HANDLE>,] I<STATEMENT> [, I<PARAMETER-LIST>] [, I<CALLBACK-LIST>] B<);>

=item RESULT = B<process(> [I<DB-HANDLE>,] I<STATEMENT> [, I<PARAMETER-LIST>] [, I<CALLBACK-LIST>] B<);>

The C<process> function will prepare your statement, bind it to any 
parameters you have provided, execute that statement and apply your 
callbacks to each row of your the result as it is collected from the
database. The function accepts the following parameters:

=over 2

=item I<DB-HANDLE>

A database handle obtained using a call to C<DBI-E<gt>connect(...)>.

The default behaviour is for C<process> to remember the last database 
handle it used and to use that handle if the parameter is omitted in a
call.

It is also possible to pre-configure the C<process> function to use a 
specific database handle and prevent it from overwriting that value.

=item I<STATEMENT>

A string containing the SQL statement to be prepared, or the handle of a
statement that has already been prepared.

Passing the statement in as a string gives you more flexibility over which
placeholder style you can choose to use. It may, however, not be the best
performing choice because C<process> will have it prepared each time.

Passing the statement in as a database handle gives you no flexibility over
placeholder style and, as a consequence, how you must bind parameters. It
does, however, give you better performance because C<process> won't bother
having it prepared.

=item I<PARAMETER-LIST>

An optional list of parameters to be bound to a prepared statement. The type of
list you will use will depend upon the placeholder style you prefer to use.

Organised by placeholder style, the following are all examples of well-formed 
parameter lists:

=over 2

=item ?-style

=over 2

=item I<VALUE-1>B<,> I<VALUE-2>B<,> B<[> I<VALUE-3>B<,> B<\%>I<ATTRS> B<],> ...B<,> I<VALUE-N>

=item B<[> I<VALUE-1>B<,> I<VALUE-2>B<,> B<[> I<VALUE-3>B<,> B<\%>I<ATTRS> B<],> ...B<,> I<VALUE-N> B<]>

=back

=item :1-style

=over 2

=item I<VALUE-1>B<,> I<VALUE-2>B<,> B<[> I<VALUE-3>B<,> B<\%>I<ATTRS> B<],> ...B<,> I<VALUE-N>

=item B<[> I<VALUE-1>B<,> I<VALUE-2>B<,> B<[> I<VALUE-3>B<,> B<\%>I<ATTRS> B<],> ...B<,> I<VALUE-N> B<]>

=back

=item :name-style

=over 2

=item I<NAME-1> B<=E<gt>> I<VALUE-1>B<,> ':I<NAME-2>' B<=E<gt>> I<VALUE-2>B<,> I<NAME-3> B<=E<gt>> B<[> I<VALUE-3>B<,> B<\%>I<ATTRS> B<],> ...B<,> I<NAME-N> B<=E<gt>> I<VALUE-N>
    
=item B<{> I<NAME-1> B<=E<gt>> I<VALUE-1>B<,> ':I<NAME-2>' B<=E<gt>> I<VALUE-2>B<,> I<NAME-3> B<=E<gt>> B<[> I<VALUE-3>B<,> B<\%>I<ATTRS> B<],> ...B<,> I<NAME-N> B<=E<gt>>I<VALUE-N> B<}>
    
=item B<[> I<NAME-1> B<=E<gt>> I<VALUE-1>B<,> ':I<NAME-2>' B<=E<gt>> I<VALUE-2>B<,> I<NAME-3> B<=E<gt>> B<[> I<VALUE-3>B<,> B<\%>I<ATTRS> B<],> ...B<,> I<NAME-N> B<=E<gt>> I<VALUE-N> B<]>
    
=back

=back

The choice to enclose your parameter list inside a list container is yours to make. 
Both options are acceptable.

When using :name-style placeholders, the choice of whether or not to prefix binding
parameter names with a leading colon (C<:>) is also yours to make. Again it doesn't 
matter.

=item I<CALLBACK-LIST>

The Callback List is an optional list of code references or anonymous subroutines 
that will be used to process your results. Result sets are processed row-by-row 
as each row is fetched. 

Each callback receives the result in C<$_[0]> and returns a result to the next
callback in the chain. The terminating result will be returned to the caller. 

A result may be manipulated during callback processing, or eliminated altogether 
by returning an empty list.

=back

The C<process> function attempts to be smart about how it handles the return
value from SQL statements that return result sets. 

When called in List Context, C<process> will return a list of rows. When 
called Scalar Context, on the other hand, things are somewhat trickier but 
predictable. If the result set contains a single row then that row is returned; 
any other outcome results in the number of rows being returned. You should reserve 
Scalar Context calls for situations in which you expect your result set to contain
a single row, or you are performing another operation for which the number of 
affected rows needs to be known.

To correctly determine the number of rows, the developer should use Perl's 
built-in C<scalar> function. For example:

    my $row_count = scalar process($dbh, 'SELECT * FROM tracks');

=back

=head1 CONFIGURATION

=over 4

=item HASH-REF = B<DBI::Fetch-E<gt>config(> [ { I<KEY-VALUE-PAIR(S)> } | I<KEY-VALUE-PAIR(S)> ] B<);>

=item HASH-REF = B<DBI::Fetch::config(> [ { I<KEY-VALUE-PAIR(S)> } | I<KEY-VALUE-PAIR(S)> ] B<);>

=item HASH-REF = B<DBI::Fetch-E<gt>push_config(> [ { I<KEY-VALUE-PAIR(S)> } | I<KEY-VALUE-PAIR(S)> ] B<);>

=item HASH-REF = B<DBI::Fetch::push_config(> [ { I<KEY-VALUE-PAIR(S)> } | I<KEY-VALUE-PAIR(S)> ] B<);>

=item HASH-REF = B<DBI::Fetch-E<gt>pop_config(> [ { I<KEY-VALUE-PAIR(S)> } | I<KEY-VALUE-PAIR(S)> ] B<);>

=item HASH-REF = B<DBI::Fetch::pop_config(> [ { I<KEY-VALUE-PAIR(S)> } | I<KEY-VALUE-PAIR(S)> ] B<);>

The C<config> functions may be used to alter the behaviour of the C<process> function
by setting or resetting the following parameters:

=over 2

=item B<auto_pop_config =E<gt>> I<BOOLEAN>

When true, this setting causes the C<process> function to discard the active
frame from the configuration stack and restore the previous configuration.

If the active frame is the B<only> frame in the configuration stack then
no action is taken and the flag is cleared.

This setting will probably only be helpful when combined with C<push_config>
as it is in the following example:

    DBI::Fetch->push_config(
        return_result_sets_as_ref => 1,
        auto_pop_config           => 1
    );

    my $result_set_ref = process($dbh, 'SELECT * FROM tracks');

The setting saves you having to call C<DBI::Fetch->pop_config()> when 
changing the behaviour of a single C<process> call.

=item B<dbh =E<gt>> I<DATABASE-HANDLE>

Sets which database handle the C<process> function will fall back to
when one is absent from the parameter list.

The default behaviour of C<process> is to "remember" the last 
database handle used. Setting the C<dbh> in this way automatically 
cancels that behaviour; clearing it reverts back to the default 
behaviour.

=item B<fetch_row_using =E<gt>> I<CODE-REFERENCE>

Sets which code is used to fetch rows from the database.

The default behaviour is for C<process> is to execute this code:

    sub { $_[0]->fetchrow_hashref('NAME_lc') }

If you don't like it, change it; but make sure your callbacks process 
the correct type of structure.
 
=item B<remember_last_used_dbh =E<gt>> I<BOOLEAN>

When true, the C<remember_last_used_dbh> setting causes the C<process>
function to remember the last database handle it used, and this is the
default behaviour.

It's useful in repeated interactions with the same database connection. The
C<process> function will fall back to the last used database handle one is
omitted from its parameter list.

When false, the C<process> function will not update the last used database
handle (whether it is set or otherwise).

=item B<return_result_sets_as_ref =E<gt>> I<BOOLEAN>

When true, this setting forces the C<process> function to return result sets
as array references, thereby removing the need for a potentially expensive 
copy operation on large sets. B<Note> that this behaviour is restricted to
result sets and the the return values from non-SELECT SQL statements.

=back

The C<config> functions come in three flavours: C<push_config>, C<pop_config> 
and plain old vanilla C<config>. Visualize the configuration as a stack of 
configurations, in which the current or active frame dictates the behaviour
of the C<process> function. 

Whereas C<config> allows you to work with the active configuration, 
C<push_config> will copy the active configuration into a new frame which
then becomes the active configuration. 

The C<pop_config> function restores the previously active configuration. You 
are prevented from accidentally discarding the original configuration. 

All three functions in this group take the same parameters (one of more 
active configuration settings) and yield a reference to the active 
configuration hash.

=back

=head1 EXPORTS

=head2 Tag group ":default"

=over 5

=item None.

=back

=head2 Tag group ":all"

=over 5

=item process

=back

=head1 BUG REPORTS

Please report any bugs to L<http://rt.cpan.org/>

=head1 AUTHOR

Iain Campbell <cpanic@cpan.org>

=head1 COPYRIGHT AND LICENCE

Copyright (C) 2012 by Iain Campbell

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.14.2 or,
at your option, any later version of Perl 5 you may have available.

=cut
