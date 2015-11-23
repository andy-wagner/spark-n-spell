import re
import math
from scipy.stats import poisson
import serialWordCheck as swc

#########################################################################################################
# CONTEXT-LEVEL CORRECTION
#
# Each sentence is modeled as a hidden Markov model. Prior probabilities (for first word in the sentence)
# and transition probabilities (for all subsequent words) are calculated when generating the main
# dictionary, using the same corpus. Emission probabilities are generated on the fly by parameterizing
# a Poisson distribution with the edit distance. The state space of possible corrections is based on the
# suggested words from the word-level correction.
#
# All probabilities are stored in log-space to avoid underflow. Pre-defined minimum values are used for
# words that are not present in the dictionary and/or probability tables.
#########################################################################################################


def get_emission_prob(edit_dist, poisson_lambda=0.01):
# Calculates emission probabilities on the fly by parameterizing Poisson (k, l),
# where k = edit distance and l=0.01

    # TODO - validate lambda parameter (taken from Verena's code)    
    return math.log(poisson.pmf(edit_dist, poisson_lambda))

# Helper function to handle KeyErrors for missing P(X1=x1)
def get_start_prob(word, start_prob, default_start_prob):
    try:
        return start_prob[word]
    except KeyError:
        return default_start_prob

# Helper function to handle KeyErrors for missing P(X2=x2|X1=x1)
def get_transition_prob(cur_word, prev_word, transition_prob, default_transition_prob):
    try:
        return transition_prob[prev_word][cur_word]
    except KeyError:
        return default_transition_prob

# Helper function to handle KeyErrors for missing P(X1, X2, X3...|E1, E2, E3...)
def get_belief(prev_word, prev_belief):
    try:
        return prev_belief[prev_word]
    except KeyError:
        return math.log(math.exp(min(prev_belief.values()))/2.) # TODO - confirm default value

# Adapted from AM207 lecture notes
# Main change is that the code does not consider entire state space (i.e. all possible words),
# but only words that are suggested as possible corrections within an edit distance of MAX_EDIT_DISTANCE.
def viterbi(words, dictionary, longest_word_length, max_edit_distance, not_found_str, \
			start_prob, default_start_prob, transition_prob, default_transition_prob, \
                num_word_suggestions=5000):
    
    V = [{}]
    path = {}
    path_context = []
    path_word = []
    
    # FOR TESTING - DELETE EVENTUALLY
    if type(words) != list:
        words = re.findall('[a-z]+', words.lower())  # separate by words by non-alphabetical characters
        
    # Character level correction
    corrections = swc.get_suggestions(words[0], dictionary, longest_word_length, max_edit_distance, \
                                  silent=True, min_count=1)

    # To ensure Viterbi can keep running
    if len(corrections) == 0:
        corrections = [(words[0], (1, 0))]
        path_word.append(not_found_str)
    else:    
        if len(corrections) > num_word_suggestions:
            corrections = corrections[0:num_word_suggestions]
        if len(corrections) > 0:
            path_word.append(corrections[0][0])  # string of most frequent word tuple
        
    # Initialize base cases (t == 0)
    for sug_word in corrections:
        
        # compute the value for all possible starting states
        V[0][sug_word[0]] = math.exp(get_start_prob(sug_word[0], start_prob, default_start_prob) \
                                     + get_emission_prob(sug_word[1][1]))
        
        # remember all the different paths (here its only one state so far)
        path[sug_word[0]] = [sug_word[0]]
 
    # normalize for numerical stability
    path_temp_sum = sum(V[0].values())
    V[0].update({k: math.log(v/path_temp_sum) for k, v in V[0].items()})
    prev_corrections = [i[0] for i in corrections]
    
    if len(words) == 1:
        path_context = [max(V[0], key=lambda i: V[0][i])]
        return path_word, path_context

    # Run Viterbi for t > 0
    for t in range(1, len(words)):

        V.append({})
        new_path = {}
        
        # Character level correction
        corrections = swc.get_suggestions(words[t], dictionary, longest_word_length, max_edit_distance, \
                        silent=True, min_count=1)
        
        # To ensure Viterbi can keep running
        if len(corrections) == 0:
            corrections = [(words[t], (1, 0))]
            path_word.append(not_found_str)
        else:
            if len(corrections) > num_word_suggestions:
                corrections = corrections[0:num_word_suggestions]
            if len(corrections) > 0:
                path_word.append(corrections[0][0])  # string of most frequent word tuple
 
        for sug_word in corrections:
        
            sug_word_emission_prob = get_emission_prob(sug_word[1][1])
            
            # compute the values coming from all possible previous states, only keep the maximum
            (prob, word) = max((get_belief(prev_word, V[t-1]) \
                            + get_transition_prob(sug_word[0], prev_word, transition_prob, default_transition_prob) \
                            + sug_word_emission_prob, prev_word) for prev_word in prev_corrections)

            # save the maximum value for each state
            V[t][sug_word[0]] = math.exp(prob)
            # remember the path we came from to get this maximum value
            new_path[sug_word[0]] = path[word] + [sug_word[0]]
            
        # normalize for numerical stability
        path_temp_sum = sum(V[t].values())
        V[t].update({k: math.log(v/path_temp_sum) for k, v in V[t].items()})
        prev_corrections = [i[0] for i in corrections]
 
        # Don't need to remember the old paths
        path = new_path
     
    (prob, word) = max((V[t][sug_word[0]], sug_word[0]) for sug_word in corrections)
    path_context = path[word]
    
    assert len(path_word) == len(path_context)

    return path_word, path_context

def correct_document_context(fname, dictionary, longest_word_length, max_edit_distance, not_found_str, \
							start_prob, default_start_prob, transition_prob, default_transition_prob, \
                            num_word_suggestions=5000):
    
    doc_word_count = 0
    unknown_word_count = 0
    corrected_word_count = 0
    mismatches = 0

    print '\nChecking spelling...'
    
    with open(fname) as file:
        
        for i, line in enumerate(file):
            
            for sentence in line.split('.'):
                
                words = re.findall('[a-z]+', sentence.lower())  # separate by words by non-alphabetical characters
                doc_word_count += len(words)
                
                if len(words) > 0:
                
                    suggestion_w, suggestion_c = viterbi(words, dictionary, longest_word_length, max_edit_distance, not_found_str, \
                                                start_prob, default_start_prob, transition_prob, default_transition_prob)

                    # Display sentences where errors have been identified
                    if (words != suggestion_w) or (words != suggestion_c):
                        
                        # Check for unknown words
                        unknown_word_count += sum([w==not_found_str for w in suggestion_w])
                        
                        # Most users will expect to see 1-indexing.
                        print '\nErrors found in line %i. \nOriginal sentence: %s' % (i+1, ' '.join(words))

                        # Word-checker and context-checker output match
                        if suggestion_w == suggestion_c:
                            print 'Word & context-level correction: %s' % (' '.join(suggestion_w))
                            corrected_word_count += sum([words[j]!=suggestion_w[j] for j in range(len(words))])
                        
                        # Word-checker and context-checker output don't match
                        else:
                            print 'Word-level correction: %s' % (' '.join(suggestion_w))
                            print 'Context-level correction: %s' % (' '.join(suggestion_c))
                            corrected_word_count += sum([(words[j]!=suggestion_w[j]) or (words[j]!=suggestion_c[j]) for j in range(len(words))])
                            mismatches += sum([suggestion_w[j] != suggestion_c[j] for j in range(len(words))])
  
    print '-----'
    print 'Total words checked: %i' % doc_word_count
    print 'Total unknown words: %i' % unknown_word_count
    print 'Total potential errors found: %i' % corrected_word_count
    print 'Total mismatches (word-level vs. context-level): %i' % mismatches