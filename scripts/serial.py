import time

import re
import math
from scipy.stats import poisson

# Functions for word-level spell-checking
import serialWordInitialize as swi
import serialWordCheck as swc

# Functions for context-level spell-checking
import serialContextInitialize as sci
import serialContextCheck as scc

if __name__ == '__main__':

	MIN_COUNT = 1
	MAX_EDIT_DISTANCE = 3
	NOT_FOUND_STR = '<not found>'

	print '\nSERIAL WORD-LEVEL CHECKING...'

	start_time = time.time()

	dictionary, longest_word_length = swi.create_dictionary("testdata/big.txt", MAX_EDIT_DISTANCE)

	run_time = time.time() - start_time

   	print '\nRuntime: %.2f seconds' % run_time

	start_time = time.time()

	swc.correct_document("testdata/test.txt", \
						dictionary, longest_word_length, MAX_EDIT_DISTANCE)

	run_time = time.time() - start_time

   	print '\nRuntime: %.2f seconds' % run_time

	print '\nSERIAL CONTEXT-LEVEL CHECKING...'

	start_time = time.time()

	dictionary, longest_word_length, start_prob, default_start_prob, transition_prob, default_transition_prob = \
		sci.create_dictionary("testdata/big.txt", MAX_EDIT_DISTANCE)

	run_time = time.time() - start_time

   	print '\nRuntime: %.2f seconds' % run_time

	start_time = time.time()

	scc.correct_document_context("testdata/test.txt", dictionary, longest_word_length, MAX_EDIT_DISTANCE, NOT_FOUND_STR, \
	                         start_prob, default_start_prob, transition_prob, default_transition_prob, min_count=MIN_COUNT)

	run_time = time.time() - start_time

   	print '\nRuntime: %.2f seconds' % run_time