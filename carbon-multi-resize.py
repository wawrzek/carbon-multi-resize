#!/usr/bin/env python

import os
from os.path import dirname, exists, join, realpath
import re
import subprocess
import sys

from carbon.conf import OrderedConfigParser
from carbon.util import pickle
import whisper

ROOT_DIR = os.environ.get('GRAPHITE_ROOT', realpath(join(dirname(__file__), '..')))
STORAGE_DIR = join(ROOT_DIR, 'storage')
WHITELISTS_DIR = join(STORAGE_DIR, 'lists')
LOCAL_DATA_DIR = join(STORAGE_DIR, 'whisper')
WHISPER_BIN = join(ROOT_DIR, 'bin')

STORAGE_SCHEMAS_CONFIG = join(ROOT_DIR, 'conf', 'storage-schemas.conf')

STORAGE_AGGREGATION_CONFIG = join(ROOT_DIR, 'conf', 'storage-aggregation.conf')

class Schema:
  def test(self, metric):
    raise NotImplementedError()

  def matches(self, metric):
    return bool( self.test(metric) )


class DefaultSchema(Schema):

  def __init__(self, name, archives):
    self.name = name
    self.archives = archives

  def test(self, metric):
    return True


class PatternSchema(Schema):

  def __init__(self, name, pattern, archives):
    self.name = name
    self.pattern = pattern
    self.regex = re.compile(pattern)
    self.archives = archives

  def test(self, metric):
    return self.regex.search(metric)

class ListSchema(Schema):

  def __init__(self, name, listName, archives):
    self.name = name
    self.listName = listName
    self.archives = archives
    self.path = join(WHITELISTS_DIR, listName)

    if exists(self.path):
      self.mtime = os.stat(self.path).st_mtime
      fh = open(self.path, 'rb')
      self.members = pickle.load(fh)
      fh.close()

    else:
      self.mtime = 0
      self.members = frozenset()

  def test(self, metric):
    if exists(self.path):
      current_mtime = os.stat(self.path).st_mtime

      if current_mtime > self.mtime:
        self.mtime = current_mtime
        fh = open(self.path, 'rb')
        self.members = pickle.load(fh)
        fh.close()

    return metric in self.members


class Archive:

  def __init__(self,secondsPerPoint,points):
    self.secondsPerPoint = int(secondsPerPoint)
    self.points = int(points)

  def __str__(self):
    return "Archive = (Seconds per point: %d, Datapoints to save: %d)" % (self.secondsPerPoint, self.points)

  def getTuple(self):
    return (self.secondsPerPoint,self.points)

  @staticmethod
  def fromString(retentionDef):
    (secondsPerPoint, points) = whisper.parseRetentionDef(retentionDef)
    return Archive(secondsPerPoint, points)


def loadStorageSchemas():
  schemaList = []
  config = OrderedConfigParser()
  config.read(STORAGE_SCHEMAS_CONFIG)

  for section in config.sections():
    options = dict( config.items(section) )
    matchAll = options.get('match-all')
    pattern = options.get('pattern')

    retentions = options['retentions'].split(',')
    archives = [ Archive.fromString(s) for s in retentions ]

    if matchAll:
      mySchema = DefaultSchema(section, archives)

    elif pattern:
      mySchema = PatternSchema(section, pattern, archives)

    archiveList = [a.getTuple() for a in archives]

    try:
      whisper.validateArchiveList(archiveList)
      schemaList.append(mySchema)
    except whisper.InvalidConfiguration, e:
      print "Invalid schemas found in %s: %s" % (section, e)

  schemaList.append(defaultSchema)
  return schemaList


def loadAggregationSchemas():
  # NOTE: This abuses the Schema classes above, and should probably be refactored.
  schemaList = []
  config = OrderedConfigParser()

  try:
    config.read(STORAGE_AGGREGATION_CONFIG)
  except IOError:
    print "%s not found, ignoring." % STORAGE_AGGREGATION_CONFIG

  for section in config.sections():
    options = dict( config.items(section) )
    matchAll = options.get('match-all')
    pattern = options.get('pattern')

    xFilesFactor = options.get('xfilesfactor')
    aggregationMethod = options.get('aggregationmethod')

    try:
      if xFilesFactor is not None:
        xFilesFactor = float(xFilesFactor)
        assert 0 <= xFilesFactor <= 1
      if aggregationMethod is not None:
        assert aggregationMethod in whisper.aggregationMethods
    except:
      print "Invalid schemas found in %s." % section
      continue

    archives = (xFilesFactor, aggregationMethod)

    if matchAll:
      mySchema = DefaultSchema(section, archives)

    elif pattern:
      mySchema = PatternSchema(section, pattern, archives)

    schemaList.append(mySchema)

  schemaList.append(defaultAggregation)
  return schemaList

defaultArchive = Archive(60, 60 * 24 * 7) #default retention for unclassified data (7 days of minutely data)
defaultSchema = DefaultSchema('default', [defaultArchive])
defaultAggregation = DefaultSchema('default', (None, None))


schemas = loadStorageSchemas()
print "Loading storage-schemas configuration from: '%s'" % STORAGE_SCHEMAS_CONFIG

agg_schemas = loadAggregationSchemas()
print "Loading storage-aggregation configuration from: '%s'" % STORAGE_AGGREGATION_CONFIG

#print schemas
#print agg_schemas

def get_archive_config(metric):
    archiveConfig = None
    xFilesFactor, aggregationMethod = None, None

    for schema in schemas:
      if schema.matches(metric):
        #print 'new metric %s matched schema %s' % (metric, schema.name)
        archiveConfig = [archive.getTuple() for archive in schema.archives]
        break

    for schema in agg_schemas:
      if schema.matches(metric):
        #print 'new metric %s matched aggregation schema %s' % (metric, schema.name)
        xFilesFactor, aggregationMethod = schema.archives
        break

    if not archiveConfig:
        raise Exception("No storage schema matched the metric '%s', check your storage-schemas.conf file." % metric)

    return (archiveConfig, xFilesFactor, aggregationMethod)

def diff_file_conf(metric, filepath):
    """
    Returns true if the actual file has parameters different from those in the configuration files
    """
    (archiveConfig, xFilesFactor, aggregationMethod) = get_archive_config(metric)

    info = whisper.info(filepath)

    if info['xFilesFactor'] != xFilesFactor or info['aggregationMethod'] != aggregationMethod:
        #print "{0} {1}".format(info['aggregationMethod'], aggregationMethod)
        #print "{0} {1}".format(info['xFilesFactor'], xFilesFactor)
        return True

    for (archivefile, archiveconf) in zip(info['archives'], archiveConfig):
        (secondsPerPoint, points) = archiveconf
        #print "{0} {1}".format(archivefile['secondsPerPoint'], secondsPerPoint)
        #print "{0} {1}".format(archivefile['points'], points)
        if archivefile['secondsPerPoint'] != secondsPerPoint or archivefile['points'] != points:
            return True

wsp_regex = re.compile('\.wsp$')
root_dir_regex = re.compile('^' + LOCAL_DATA_DIR + os.sep)
dir_sep_regex = re.compile(os.sep)

for root, dirs, files in os.walk(LOCAL_DATA_DIR):
    for filename in [f for f in files if wsp_regex.search(f)]:
        filepath = join(root, filename)
        metric = dir_sep_regex.sub('.', wsp_regex.sub('', root_dir_regex.sub('', filepath)))
        print "Processing {0}".format(filepath)
        if diff_file_conf(metric, filepath):
            #there is a difference and we need to resize the whisper file
            (archiveConfig, xFilesFactor, aggregationMethod) = get_archive_config(metric)
            command_args = [WHISPER_BIN + '/whisper-resize.py', filepath]
            for (secondsPerPoint, points) in archiveConfig:
                command_args.append("{0}:{1}".format(secondsPerPoint, points))

            command_args.append('--nobackup')

            if aggregationMethod:
                command_args.append('--aggregationMethod={0}'.format(aggregationMethod))

            if xFilesFactor:
                command_args.append('--xFilesFactor={0}'.format(xFilesFactor))

            #print ' '.join(command_args)
            subprocess.check_output(command_args)
