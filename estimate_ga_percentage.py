from __future__ import division
import glob
import gzip
##
from collections import defaultdict


freq = {
    'GA': defaultdict(int),
    'NoGA': defaultdict(int)
}


def get_ga_counts():
  global freq
  #
  for fn in sorted(glob.glob('ga_out/*.gz')):
    print 'Processing {}'.format(fn)
    with gzip.open(fn, 'rb') as f:
      for line in f:
        key, count = line.strip().split('\t')
        page, ga_state = key.split(' ')
        freq[ga_state][page] = int(count)

if __name__ == '__main__':
  get_ga_counts()
  print 'Domains in GA:', len(freq['GA'])
  print 'Domains in NoGA:', len(freq['NoGA'])
  print 'Pages in GA:', sum(freq['GA'].itervalues())
  print 'Pages in NoGA:', sum(freq['NoGA'].itervalues())

  # Calculate the total number of links and sum all links which have an endpoint in GA
  has_ga, total, skipped = 0, 0, 0
  for fn in sorted(glob.glob('lg_out/*.gz')):
    print 'Processing {}'.format(fn)
    print '{} / {} (with {} skipped)'.format(int(has_ga), total, skipped)
    with gzip.open(fn, 'rb') as f:
      for i, line in enumerate(f):
        key, count = line.strip().split('\t')
        if i % 100000 == 0:
          print key
        A, B = key.split(' -> ')
        n = int(count)
        # The denominator will be zero if one of the pages has no processed pages
        if (A not in freq['GA'] and A not in freq['NoGA']) or (B not in freq['GA'] and B not in freq['NoGA']):
          skipped += n
          continue
        z = (freq['GA'][A] + freq['NoGA'][A]) * (freq['GA'][B] + freq['NoGA'][B])
        if z > 0:
          has_ga += (
              (freq['GA'][A] * freq['NoGA'][B]) / z +  # GA -> NoGA
              (freq['GA'][A] * freq['GA'][B]) / z +  # GA -> GA
              (freq['NoGA'][A] * freq['GA'][B]) / z    # NoGA -> GA
          ) * n
          total += n
  print
  print 'Final Result:'
  print '{} / {} (with {} skipped)'.format(int(has_ga), total, skipped)
