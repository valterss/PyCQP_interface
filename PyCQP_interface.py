#! /usr/bin/env python
# -*- coding: utf-8 -*-

# Name of this module: PyCQP
# Version 2.0 (Febr. 2008)
# Joerg Asmussen, DSL

# Import external standard Python modules used by this module:
import sys
import os
import re
import random
import time
from six.moves import _thread as thread
import tempfile
import logging

# Modules for running CQP as child process and pipe i/o
# (standard in newer Python):
import subprocess
import select

# GLOBAL CONSTANTS OF MODULE:
cProgressControlCycle = 30  # secs between each progress control cycle
cMaxRequestProcTime = 40  # max secs for processing a user request

# ERROR MESSAGE TYPES:


class ErrCQP:
    """Handle CQP error message."""

    def __init__(self, msg):
        """Class constructor."""
        self.msg = msg.rstrip()


class ErrKilled:
    """Handle errors when process is killed."""

    def __init__(self, msg):
        """Class constructor."""
        self.msg = msg.rstrip()


class CQP:
    """Wrapper for CQP."""

    def _progressController(self):
        """Control the progess.

        CREATED: 2008-02

        This method is run as a thread.
        At certain intervals (cProgressControlCycle), it controls how long the
        CQP process of the current user has spent on processing this user's
        latest CQP command. If this time exceeds a certain maximun
        (cMaxRequestProcTime), this method kills the CQP process.
        """
        self.runController = True
        while self.runController:
            time.sleep(cProgressControlCycle)
            if self.execStart is not None:
                if time.time() - self.execStart > cMaxRequestProcTime *\
                 self.maxProcCycles:
                    self.__logger.warning('PROGRESS CONTROLLER IDENTIFIED BLOCKING CQP PROCESS ID %d', self.CQP_process.pid)
                    # os.kill(self.CQP_process.pid, SIGKILL) - doesn't work!
                    # os.popen("kill -9 " + str(self.CQP_process.pid))  # works!
                    self.CQP_process.kill()
                    self.__logger.info("=> KILLED!")
                    self.CQPrunning = False
                    break

    def __init__(self, bin=None, options=''):
        """Class constructor."""
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.execStart = time.time()
        self.maxProcCycles = 1.0
        # start CQP as a child process of this wrapper
        if bin is None:
            self.__logger.error("Path to CQP binaries undefined")
            sys.exit(1)
        self.CQP_process = subprocess.Popen(bin + ' ' + options,
                                            shell=True,
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE,
                                            universal_newlines=True,
                                            close_fds=True)
        self.CQPrunning = True
        thread.start_new_thread(self._progressController, ())
        # "cqp -c" should print version on startup:
        version_string = self.CQP_process.stdout.readline()
        version_string = version_string.rstrip()  # Equivalent to Perl's chomp
        self.CQP_process.stdout.flush()
        self.__logger.info("Test " + version_string)
        version_regexp = re.compile(r'^CQP\s+(?:\w+\s+)*([0-9]+)\.([0-9]+)(?:\.b?([0-9]+))?(?:\s+(.*))?$')
        match = version_regexp.match(version_string)
        if not match:
            self.__logger.error("CQP backend startup failed")
            sys.exit(1)
        self.major_version = int(match.group(1))
        self.minor_version = int(match.group(2))
        self.beta_version = int(match.group(3))
        self.compile_date = match.group(4)

        # We need cqp-2.2.b41 or newer (for query lock):
        if not (self.major_version >= 3 or
                (self.major_version == 2 and
                 self.minor_version == 2 and
                 self.beta_version >= 41)):
            self.__logger.error("CQP version too old: %s", version_string)
            sys.exit(1)

        # Error handling:
        self.error_handler = None
        self.status = 'ok'
        self.error_message = ''  # we store compound error messages as a STRING
        self.errpipe = self.CQP_process.stderr.fileno()

        # CQP defaults:
        self.Exec('set PrettyPrint off')
        self.execStart = None

    def Terminate(self):
        """Terminate controller thread, must be called before deleting CQP."""
        self.execStart = None
        self.runController = False

    def SetProcCycles(self, procCycles):
        """Set procCycles."""
        self.__logger.info("Setting procCycles to %d", procCycles)
        self.maxProcCycles = procCycles
        return int(self.maxProcCycles * cMaxRequestProcTime)

    def __del__(self):
        self.exit_cqp()

    def exit_cqp(self):
        """Stop running CQP instance."""
        self.Terminate()
        if self.CQPrunning:
            self.CQPrunning = False
            self.execStart = time.time()
            self.__logger.debug("Shutting down CQP backend (pid: %d)...", self.CQP_process.pid)
            #self.CQP_process.stdin.write('exit;')  # exits CQP backend
            self.CQP_process.kill()
            self.__logger.debug("Done - CQP object deleted.")
            self.execStart = None

    def Exec(self, cmd):
        """Execute CQP command.

        The method takes as input a command string and sends it
        to the CQP child process
        """
        self.execStart = time.time()
        self.status = 'ok'
        cmd = cmd.rstrip()  # Equivalent to Perl's 'chomp'
        cmd = re.sub(r';\s*$', r'', cmd)
        self.__logger.debug("CQP <<%s;", cmd)
        try:
            self.CQP_process.stdin.write(cmd + '; .EOL.;\n')
        except IOError:
            return None
        # In CQP.pm lines are appended to a list @result.
        # This implementation prefers a string structure instead
        # because output from this module is meant to be transferred
        # accross a server connection. To enable the development of
        # client modules written in any language, the server only emits
        # strings which then are to be structured by the client module.
        # The server does not emit pickled data according to some
        # language dependent protocol.
        self.CQP_process.stdin.flush()
        result = []
        while self.CQPrunning:
            ln = self.CQP_process.stdout.readline()
            ln = ln.strip()  # strip off whitespace from start and end of line
            if re.match(r'-::-EOL-::-', ln):
                self.__logger.debug("CQP " + "-" * 60)
                break
            self.__logger.debug("CQP >> %s", ln)
            if ln != '':
                result.append(ln)
            self.CQP_process.stdout.flush()
        self.Checkerr()
        self.execStart = None
        result = '\n'.join(result)
        result = result.rstrip()  # strip off whitespace from EOL (\n)
        return result

    def Query(self, query):
        """Execute query in safe mode (query lock)."""
        result = []
        key = str(random.randint(1, 1000000))
        errormsg = ''  # collect CQP error messages AS STRING
        ok = True      # check if any error occurs
        self.Exec('set QueryLock ' + key)  # enter query lock mode
        if self.status != 'ok':
            errormsg = errormsg + self.error_message
            ok = False
        result = self.Exec(query)
        if self.status != 'ok':
            errormsg = errormsg + self.error_message.decode('utf-8')
            ok = ok and False
        self.Exec('unlock ' + key)  # unlock with random key
        if self.status != 'ok':
            errormsg = errormsg + self.error_message
            ok = ok and False
        # Set error status & error message:
        if ok:
            self.status = 'ok'
        else:
            self.status = 'error'
            self.error_message = errormsg
        return result

    def Dump(self, subcorpus='Last', first=None, last=None):
        """Dump named query result into table of corpus positions."""
        if first is None and last is None:
            result = self.Exec('dump ' + subcorpus + ";")
        elif ((not isinstance(first, int) and first is not None) or
                (not isinstance(last, int) and last is not None)):
            sys.stderr.write(
                            "ERROR: Invalid value for first (" +
                            str(first) + ") or last (" + str(last) +
                            ") line in Dump() method\n")
            sys.exit(1)
        elif isinstance(first, int) and isinstance(last, int):
            if first > last:
                sys.stderr.write(
                    "ERROR: Invalid value for first line (first = " +
                    str(first) + " > last = " + str(last) +
                    ") in Dump() method\n")
                sys.exit(1)
            else:
                result = self.Exec(
                    'dump ' +
                    subcorpus +
                    " " +
                    str(first) +
                    " " +
                    str(last))
        else:
            if first is not None and last is None:
                last = first
            elif last is not None and first is None:
                first = last
            result = self.Exec(
                'dump ' +
                subcorpus +
                " " +
                str(first) +
                " " +
                str(last) +
                ";")
        result = [x.split('\t') for x in result.split('\n')]
        return result

    def Undump(self, subcorpus='Last', table=[]):
        """Undump named query result from table of corpus positions."""
        wth = ''  # undump with target and keyword
        n_el = None  # number of anchors for each match (from first row)
        n_matches = len(table)  # number of matches (= remaining arguments)
        # We have to read undump table from temporary file:
        tf = tempfile.NamedTemporaryFile(prefix='pycqp_undump_')
        filename = tf.name
        tf.write(str(n_matches) + '\n')
        for row in table:
            row_el = len(row)
            if n_el is None:
                n_el = row_el
                if (n_el < 2) or (n_el > 4):
                    self.__logger.error(
                        "Row arrays in undump table must have " +
                        "between 2 and 4 elements (first row has %s elements)", n_el)
                    sys.exit(1)
                if n_el >= 3:
                    wth = 'with target'
                if n_el == 4:
                    wth = wth + ' keyword'
            elif row_el != n_el:
                self.__logger.error(
                    "ERROR: All rows in undump table must have the same " +
                    "length (first row = %s, this row = %s)", n_el, row_el)
                sys.exit(1)
            tf.write('\t'.join(row) + '\n')
        tf.close()
        # Send undump command with filename of temporary file:
        self.Exec("undump " + subcorpus + " " + wth + " < '" + filename + "'")
        tf.delete()

    def Group(self, subcorpus='Last',
              spec1='match.word', spec2='', cutoff='1'):
        """Compute frequency distribution over attribute values.

        (single values or pairs) using group command.

        Note that the arguments are specified in the logical order,
        in contrast to "group"
        """
        spec2_regexp = re.compile(r'^[0-9]+$')
        if spec2_regexp.match(spec2):
            cutoff = spec2
            spec2 = ''
        spec_regexp = re.compile(
          r'^(match|matchend|target[0-9]?|keyword)\.([A-Za-z0-9_-]+)$')
        match = re.match(spec_regexp, spec1)
        if not match:
            self.__logger.error("Invalid key '%s' in Group() method", spec1)
            sys.exit(1)
        spec1 = match.group(1) + ' ' + match.group(2)
        if spec2 != '':
            match = re.match(spec_regexp, spec2)
            if not match:
                self.__logger.error("Invalid key '%s' in Group() method", spec2)
                sys.exit(1)
            spec2 = match.group(1) + ' ' + match.group(2)
            cmd = 'group ' + subcorpus + ' ' + spec2 + ' by ' + spec1 + \
                  ' cut ' + cutoff
        else:
            cmd = 'group ' + subcorpus + ' ' + spec1 + ' cut ' + cutoff
        result = self.Exec(cmd)
        return result

    def Count(self, subcorpus='Last', sort_clause=None, cutoff=1):
        """Compute frequency distribution for match strings.

        Based on sort clause.
        """
        if sort_clause is None:
            self.__logger.error("Parameter 'sort_clause' undefined in Count() method")
            sys.exit(1)
        return self.Exec(
            'count ' +
            subcorpus +
            ' by ' +
            sort_clause +
            ' cut ' +
            str(cutoff))

    def Checkerr(self):
        """Check CQP's stderr stream for error messages.

        (returns true if there was an error).
        OBS! In CQP.pm the error_message is stored in a list,
        In PyCQP_interface.pm we use a string (which better can be sent
        accross the server line).
        """
        ready = select.select([self.errpipe], [], [], 0)
        if self.errpipe in ready[0]:
            # We've got something on stderr -> an error must have occurred:
            self.status = 'error'
            self.error_message = self.Readerr()
        return not self.Ok()

    def Readerr(self):
        """Read all available lines from CQP's stderr stream."""
        return os.read(self.errpipe, 16384)

    def Status(self):
        """Read the CQP object's (error) status."""
        return self.status

    def Ok(self):
        """Simplified interface for checking for CQP errors."""
        if self.CQPrunning:
            return self.Status() == 'ok'
        else:
            return False

    def Error_message(self):
        """Return the CQP error message."""
        if self.CQPrunning:
            return ErrCQP(self.error_message)
        else:
            msgKilled = '**** CQP KILLED ***\n\
            CQP COULD NOT PROCESS YOUR REQUEST\n'
            return ErrKilled(msgKilled + self.error_message)

    def Error(self, msg):
        """Processe/output error messages.

        (optionally run through user-defined error handler)
        """
        if self.error_handler is not None:
            self.error_handler(msg)
        else:
            self.__logger.info(msg)

    def Set_error_handler(self, handler=None):
        """Set user-defined error handler."""
        self.error_handler = handler
