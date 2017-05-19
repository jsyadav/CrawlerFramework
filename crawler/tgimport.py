#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys,re
import os
from ConfigParser import ConfigParser

config = ConfigParser()
config.readfp(open('/home/deployer/Crawler/crawler.cfg'))
