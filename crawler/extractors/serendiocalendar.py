from parsedatetime import parsedatetime
from parsedatetime.parsedatetime import Calendar,_debug
import time
import datetime

class SerendioCalendar(Calendar):
    def _buildTime(self, source, quantity, modifier, units,postTime=None):
        """
        Take C{quantity}, C{modifier} and C{unit} strings and convert them into values.
        After converting, calcuate the time and return the adjusted sourceTime.

        @type  source:   time
        @param source:   time to use as the base (or source)
        @type  quantity: string
        @param quantity: quantity string
        @type  modifier: string
        @param modifier: how quantity and units modify the source time
        @type  units:    string
        @param units:    unit of the quantity (i.e. hours, days, months, etc)

        @rtype:  struct_time
        @return: C{struct_time} of the calculated time
        """
        if _debug:
            print '_buildTime: [%s][%s][%s]' % (quantity, modifier, units)

        if source is None and postTime:
            source = postTime
        else:
            source = time.localtime()

        if quantity is None:
            quantity = ''
        else:
            quantity = quantity.strip()

        if len(quantity) == 0:
            qty = 1
        else:
            try:
                qty = int(quantity)
            except ValueError:
                qty = 0

        if modifier in self.ptc.Modifiers:
            qty = qty * self.ptc.Modifiers[modifier]

            if units is None or units == '':
                units = 'dy'

        # plurals are handled by regex's (could be a bug tho)

        (yr, mth, dy, hr, mn, sec, _, _, _) = source

        start  = datetime.datetime(yr, mth, dy, hr, mn, sec)
        target = start

        if units.startswith('y'):
            target        = self.inc(start, year=qty)
            self.dateFlag = 1
        elif units.endswith('th') or units.endswith('ths'):
            target        = self.inc(start, month=qty)
            self.dateFlag = 1
        else:
            if units.startswith('d'):
                target        = start + datetime.timedelta(days=qty)
                self.dateFlag = 1
            elif units.startswith('h'):
                target        = start + datetime.timedelta(hours=qty)
                self.timeFlag = 2
            elif units.startswith('m'):
                target        = start + datetime.timedelta(minutes=qty)
                self.timeFlag = 2
            elif units.startswith('s'):
                target        = start + datetime.timedelta(seconds=qty)
                self.timeFlag = 2
            elif units.startswith('w'):
                target        = start + datetime.timedelta(weeks=qty)
                self.dateFlag = 1

        return target.timetuple()
    def parseDate(self, dateString,postTime=None):
        """
        Parse short-form date strings::

            '05/28/2006' or '04.21'

        @type  dateString: string
        @param dateString: text to convert to a C{datetime}

        @rtype:  struct_time
        @return: calculated C{struct_time} value of dateString
        """
        if not postTime:
            yr, mth, dy, hr, mn, sec, wd, yd, isdst = time.localtime()
        else:
            yr, mth, dy, hr, mn, sec, wd, yd, isdst= postTime
    

        # values pulled from regex's will be stored here and later
        # assigned to mth, dy, yr based on information from the locale
        # -1 is used as the marker value because we want zero values
        # to be passed thru so they can be flagged as errors later
        v1 = -1
        v2 = -1
        v3 = -1

        s = dateString
        m = self.ptc.CRE_DATE2.search(s)
        if m is not None:
            index = m.start()
            v1    = int(s[:index])
            s     = s[index + 1:]

        m = self.ptc.CRE_DATE2.search(s)
        if m is not None:
            index = m.start()
            v2    = int(s[:index])
            v3    = int(s[index + 1:])
        else:
            v2 = int(s.strip())

        v = [ v1, v2, v3 ]
        d = { 'm': mth, 'd': dy, 'y': yr }

        for i in range(0, 3):
            n = v[i]
            c = self.ptc.dp_order[i]
            if n >= 0:
                d[c] = n

        # if the year is not specified and the date has already
        # passed, increment the year
        if v3 == -1 and ((mth > d['m']) or (mth == d['m'] and dy > d['d'])):
            yr = d['y'] + 1
        else:
            yr  = d['y']

        mth = d['m']
        dy  = d['d']

        # birthday epoch constraint
        if yr < self.ptc.BirthdayEpoch:
            yr += 2000
        elif yr < 100:
            yr += 1900

        if _debug:
            print 'parseDate: ', yr, mth, dy, self.ptc.daysInMonth(mth, yr)

        if (mth > 0 and mth <= 12) and \
           (dy > 0 and dy <= self.ptc.daysInMonth(mth, yr)):
            sourceTime = (yr, mth, dy, hr, mn, sec, wd, yd, isdst)
        else:
            self.dateFlag = 0
            self.timeFlag = 0
            # return current time if date
            if postTime:
                sourceTime = postTime.timetuple()
            else:
                sourceTime    = time.localtime()                                
        return sourceTime

    def parseDateText(self, dateString,postTime=None):
        """
        Parse long-form date strings::

            'May 31st, 2006'
            'Jan 1st'
            'July 2006'

        @type  dateString: string
        @param dateString: text to convert to a datetime

        @rtype:  struct_time
        @return: calculated C{struct_time} value of dateString
        """
        
        if  postTime is None:
            yr, mth, dy, hr, mn, sec, wd, yd, isdst = time.localtime()
        else:
            yr, mth, dy, hr, mn, sec, wd, yd, isdst = postTime

        currentMth = mth
        currentDy  = dy

        s   = dateString.lower()
        m   = self.ptc.CRE_DATE3.search(s)
        if m and m.group('mthname')!=None:
            mth = m.group('mthname')
            mth = self.ptc.MonthOffsets[mth]

        if m and m.group('day') !=  None:
            dy = int(m.group('day'))
        else:
            dy = 1

        if m and m.group('year') !=  None:
            yr = int(m.group('year'))

            # birthday epoch constraint
            if yr < self.ptc.BirthdayEpoch:
                yr += 2000
            elif yr < 100:
                yr += 1900

        elif (mth < currentMth) or (mth == currentMth and dy < currentDy):
            # if that day and month have already passed in this year,
            # then increment the year by 1
            yr += 1

        if dy > 0 and dy <= self.ptc.daysInMonth(mth, yr):
            sourceTime = (yr, mth, dy, hr, mn, sec, wd, yd, isdst)
        else:
            # Return current time if date string is invalid
            self.dateFlag = 0
            self.timeFlag = 0
            if postTime:
                #print "Hi"
                sourceTime=postTime
            else:
		
                sourceTime = time.localtime()

        return sourceTime
    def evalRanges(self, datetimeString):
        """
        Evaluate the C{datetimeString} text and determine if
        it represents a date or time range.

        @type  datetimeString: string
        @param datetimeString: datetime text to evaluate
        @type  sourceTime:     struct_time
        @param sourceTime:     C{struct_time} value to use as the base

        @rtype:  tuple
        @return: tuple of: start datetime, end datetime and the invalid flag
        """
        startTime = ''
        endTime   = ''
        startDate = ''
        endDate   = ''
        rangeFlag = 0

        s = datetimeString.strip().lower()

        if self.ptc.rangeSep in s:
            s = s.replace(self.ptc.rangeSep, ' %s ' % self.ptc.rangeSep)
            s = s.replace('  ', ' ')

        m = self.ptc.CRE_TIMERNG1.search(s)
        if m is not None:
            rangeFlag = 1
        else:
            m = self.ptc.CRE_TIMERNG2.search(s)
            if m is not None:
                rangeFlag = 2
            else:
                m = self.ptc.CRE_TIMERNG4.search(s)
                if m is not None:
                    rangeFlag = 7
                else:
                    m = self.ptc.CRE_TIMERNG3.search(s)
                    if m is not None:
                        rangeFlag = 3
                    else:
                        m = self.ptc.CRE_DATERNG1.search(s)
                        if m is not None:
                            rangeFlag = 4
                        else:
                            m = self.ptc.CRE_DATERNG2.search(s)
                            if m is not None:
                                rangeFlag = 5
                            else:
                                m = self.ptc.CRE_DATERNG3.search(s)
                                if m is not None:
                                    rangeFlag = 6

        if _debug:
            print 'evalRanges: rangeFlag =', rangeFlag, '[%s]' % s

        if m is not None:
            if (m.group() != s):
                # capture remaining string
                parseStr = m.group()
                chunk1   = s[:m.start()]
                chunk2   = s[m.end():]
                s        = '%s %s' % (chunk1, chunk2)
                flag     = 1

                sourceTime, flag = self.parse(s, sourceTime)

                if flag == 0:
                    sourceTime = None
            else:
                parseStr = s

        if rangeFlag == 1:
            m                = re.search(self.ptc.rangeSep, parseStr)
            startTime, sflag = self.parse((parseStr[:m.start()]),       sourceTime,postTime=postTime)
            endTime, eflag   = self.parse((parseStr[(m.start() + 1):]), sourceTime,postTime=postTime)

            if (eflag != 0)  and (sflag != 0):
                return (startTime, endTime, 2)

        elif rangeFlag == 2:
            m                = re.search(self.ptc.rangeSep, parseStr)
            startTime, sflag = self.parse((parseStr[:m.start()]),       sourceTime,postTime)
            endTime, eflag   = self.parse((parseStr[(m.start() + 1):]), sourceTime,postTime)

            if (eflag != 0)  and (sflag != 0):
                return (startTime, endTime, 2)

        elif rangeFlag == 3 or rangeFlag == 7:
            m = re.search(self.ptc.rangeSep, parseStr)
            # capturing the meridian from the end time
            if self.ptc.usesMeridian:
                ampm = re.search(self.ptc.am[0], parseStr)

                # appending the meridian to the start time
                if ampm is not None:
                    startTime, sflag = self.parse((parseStr[:m.start()] + self.ptc.meridian[0]), sourceTime)
                else:
                    startTime, sflag = self.parse((parseStr[:m.start()] + self.ptc.meridian[1]), sourceTime)
            else:
                startTime, sflag = self.parse((parseStr[:m.start()]), sourceTime)

            endTime, eflag = self.parse(parseStr[(m.start() + 1):], sourceTime)

            if (eflag != 0)  and (sflag != 0):
                return (startTime, endTime, 2)

        elif rangeFlag == 4:
            m                = re.search(self.ptc.rangeSep, parseStr,postTime)
            startDate, sflag = self.parse((parseStr[:m.start()]),       sourceTime,postTime)
            endDate, eflag   = self.parse((parseStr[(m.start() + 1):]), sourceTime,postTime)

            if (eflag != 0)  and (sflag != 0):
                return (startDate, endDate, 1)

        elif rangeFlag == 5:
            m       = re.search(self.ptc.rangeSep, parseStr)
            endDate = parseStr[(m.start() + 1):]

            # capturing the year from the end date
            date    = self.ptc.CRE_DATE3.search(endDate)
            endYear = date.group('year')

            # appending the year to the start date if the start date
            # does not have year information and the end date does.
            # eg : "Aug 21 - Sep 4, 2007"
            if endYear is not None:
                startDate = (parseStr[:m.start()]).strip()
                date      = self.ptc.CRE_DATE3.search(startDate)
                startYear = date.group('year')

                if startYear is None:
                    startDate = startDate + ', ' + endYear
            else:
                startDate = parseStr[:m.start()]

            startDate, sflag = self.parse(startDate, sourceTime,postTime=postTime)
            endDate, eflag   = self.parse(endDate, sourceTime,postTime=postTime)

            if (eflag != 0)  and (sflag != 0):
                return (startDate, endDate, 1)

        elif rangeFlag == 6:
            m = re.search(self.ptc.rangeSep, parseStr)

            startDate = parseStr[:m.start()]

            # capturing the month from the start date
            mth = self.ptc.CRE_DATE3.search(startDate)
            mth = mth.group('mthname')

            # appending the month name to the end date
            endDate = mth + parseStr[(m.start() + 1):]

            startDate, sflag = self.parse(startDate, sourceTime,postTime=postTime)
            endDate, eflag   = self.parse(endDate, sourceTime,postTime=postTime)

            if (eflag != 0)  and (sflag != 0):
                return (startDate, endDate, 1)
        else:
            # if range is not found
            if sourcetime:
                sourceTime = postTime
            else: 
                sourceTime = time.localtime()

            return (sourceTime, sourceTime, 0)
    def _evalModifier(self, modifier, chunk1, chunk2, sourceTime,postTime=None):
        """
        Evaluate the C{modifier} string and following text (passed in
        as C{chunk1} and C{chunk2}) and if they match any known modifiers
        calculate the delta and apply it to C{sourceTime}.

        @type  modifier:   string
        @param modifier:   modifier text to apply to sourceTime
        @type  chunk1:     string
        @param chunk1:     first text chunk that followed modifier (if any)
        @type  chunk2:     string
        @param chunk2:     second text chunk that followed modifier (if any)
        @type  sourceTime: struct_time
        @param sourceTime: C{struct_time} value to use as the base

        @rtype:  tuple
        @return: tuple of: remaining text and the modified sourceTime
        """
        offset = self.ptc.Modifiers[modifier]

        if sourceTime is not None and postTime is None:
            (yr, mth, dy, hr, mn, sec, wd, yd, isdst) = sourceTime
        elif postTime is not None:
            (yr, mth, dy, hr, mn, sec, wd, yd, isdst) = postTime
        else:
            (yr, mth, dy, hr, mn, sec, wd, yd, isdst) = time.localtime()

        # capture the units after the modifier and the remaining
        # string after the unit
        m = self.ptc.CRE_REMAINING.search(chunk2)
        if m is not None:
            index  = m.start() + 1
            unit   = chunk2[:m.start()]
            chunk2 = chunk2[index:]
        else:
            unit   = chunk2
            chunk2 = ''

        flag = False

        if unit == 'month' or \
           unit == 'mth' or \
           unit == 'm':
            if offset == 0:
                dy         = self.ptc.daysInMonth(mth, yr)
                sourceTime = (yr, mth, dy, 9, 0, 0, wd, yd, isdst)
            elif offset == 2:
                # if day is the last day of the month, calculate the last day
                # of the next month
                if dy == self.ptc.daysInMonth(mth, yr):
                    dy = self.ptc.daysInMonth(mth + 1, yr)

                start      = datetime.datetime(yr, mth, dy, 9, 0, 0)
                target     = self.inc(start, month=1)
                sourceTime = target.timetuple()
            else:
                start      = datetime.datetime(yr, mth, 1, 9, 0, 0)
                target     = self.inc(start, month=offset)
                sourceTime = target.timetuple()

            flag = True
            self.dateFlag = 1

        if unit == 'week' or \
             unit == 'wk' or \
             unit == 'w':
            if offset == 0:
                start      = datetime.datetime(yr, mth, dy, 17, 0, 0)
                target     = start + datetime.timedelta(days=(4 - wd))
                sourceTime = target.timetuple()
            elif offset == 2:
                start      = datetime.datetime(yr, mth, dy, 9, 0, 0)
                target     = start + datetime.timedelta(days=7)
                sourceTime = target.timetuple()
            else:
                return self._evalModifier(modifier, chunk1, "monday " + chunk2, sourceTime,postTime=postTime)

            flag          = True
            self.dateFlag = 1

        if unit == 'day' or \
            unit == 'dy' or \
            unit == 'd':
            if offset == 0:
                sourceTime    = (yr, mth, dy, 17, 0, 0, wd, yd, isdst)
                self.timeFlag = 2
            elif offset == 2:
                start      = datetime.datetime(yr, mth, dy, hr, mn, sec)
                target     = start + datetime.timedelta(days=1)
                sourceTime = target.timetuple()
            else:
                start      = datetime.datetime(yr, mth, dy, 9, 0, 0)
                target     = start + datetime.timedelta(days=offset)
                sourceTime = target.timetuple()

            flag          = True
            self.dateFlag = 1

        if unit == 'hour' or \
           unit == 'hr':
            if offset == 0:
                sourceTime = (yr, mth, dy, hr, 0, 0, wd, yd, isdst)
            else:
                start      = datetime.datetime(yr, mth, dy, hr, 0, 0)
                target     = start + datetime.timedelta(hours=offset)
                sourceTime = target.timetuple()

            flag          = True
            self.timeFlag = 2

        if unit == 'year' or \
             unit == 'yr' or \
             unit == 'y':
            if offset == 0:
                sourceTime = (yr, 12, 31, hr, mn, sec, wd, yd, isdst)
            elif offset == 2:
                sourceTime = (yr + 1, mth, dy, hr, mn, sec, wd, yd, isdst)
            else:
                sourceTime = (yr + offset, 1, 1, 9, 0, 0, wd, yd, isdst)

            flag          = True
            self.dateFlag = 1

        if flag == False:
            m = self.ptc.CRE_WEEKDAY.match(unit)
            if m is not None:
                wkdy          = m.group()
                self.dateFlag = 1

                if modifier == 'eod':
                    # Calculate the  upcoming weekday
                    self.modifierFlag = False
                    (sourceTime, _)   = self.parse(wkdy, sourceTime)
                    sources           = self.ptc.buildSources(sourceTime)
                    self.timeFlag     = 2

                    if modifier in sources:
                        sourceTime = sources[modifier]

                else:
                    wkdy       = self.ptc.WeekdayOffsets[wkdy]
                    diff       = self._CalculateDOWDelta(wd, wkdy, offset,
                                                         self.ptc.DOWParseStyle,
                                                         self.ptc.CurrentDOWParseStyle)
                    start      = datetime.datetime(yr, mth, dy, 9, 0, 0)
                    target     = start + datetime.timedelta(days=diff)
                    sourceTime = target.timetuple()

                flag          = True
                self.dateFlag = 1

        if not flag:
            m = self.ptc.CRE_TIME.match(unit)
            if m is not None:
                self.modifierFlag = False
                (yr, mth, dy, hr, mn, sec, wd, yd, isdst), _ = self.parse(unit,postTime=postTime)

                start      = datetime.datetime(yr, mth, dy, hr, mn, sec)
                target     = start + datetime.timedelta(days=offset)
                sourceTime = target.timetuple()
                flag       = True
            else:
                self.modifierFlag = False

                # check if the remaining text is parsable and if so,
                # use it as the base time for the modifier source time
                t, flag2 = self.parse('%s %s' % (chunk1, unit), sourceTime,postTime=postTime)

                if flag2 != 0:
                    sourceTime = t

                sources = self.ptc.buildSources(sourceTime,postTime=postTime)

                if modifier in sources:
                    sourceTime    = sources[modifier]
                    flag          = True
                    self.timeFlag = 2

        # if the word after next is a number, the string is more than likely
        # to be "next 4 hrs" which we will have to combine the units with the
        # rest of the string
        if not flag:
            if offset < 0:
                # if offset is negative, the unit has to be made negative
                unit = '-%s' % unit

            chunk2 = '%s %s' % (unit, chunk2)

        self.modifierFlag = False

        #return '%s %s' % (chunk1, chunk2), sourceTime
        return '%s' % chunk2, sourceTime


    def _evalModifier2(self, modifier, chunk1 , chunk2, sourceTime,postTime=None):
        """
        Evaluate the C{modifier} string and following text (passed in
        as C{chunk1} and C{chunk2}) and if they match any known modifiers
        calculate the delta and apply it to C{sourceTime}.

        @type  modifier:   string
        @param modifier:   modifier text to apply to C{sourceTime}
        @type  chunk1:     string
        @param chunk1:     first text chunk that followed modifier (if any)
        @type  chunk2:     string
        @param chunk2:     second text chunk that followed modifier (if any)
        @type  sourceTime: struct_time
        @param sourceTime: C{struct_time} value to use as the base

        @rtype:  tuple
        @return: tuple of: remaining text and the modified sourceTime
        """
        offset = self.ptc.Modifiers[modifier]
        digit  = r'\d+'

        self.modifier2Flag = False

        # If the string after the negative modifier starts with digits,
        # then it is likely that the string is similar to ' before 3 days'
        # or 'evening prior to 3 days'.
        # In this case, the total time is calculated by subtracting '3 days'
        # from the current date.
        # So, we have to identify the quantity and negate it before parsing
        # the string.
        # This is not required for strings not starting with digits since the
        # string is enough to calculate the sourceTime
        if chunk2 != '':
            if offset < 0:
                m = re.match(digit, chunk2.strip())
                if m is not None:
                    qty    = int(m.group()) * -1
                    chunk2 = chunk2[m.end():]
                    chunk2 = '%d%s' % (qty, chunk2)

            sourceTime, flag1 = self.parse(chunk2, sourceTime,postTime=postTime)
            if flag1 == 0:
                flag1 = True
            else:
                flag1 = False
            flag2 = False
        else:
            flag1 = False

        if chunk1 != '':
            if offset < 0:
                m = re.search(digit, chunk1.strip())
                if m is not None:
                    qty    = int(m.group()) * -1
                    chunk1 = chunk1[m.end():]
                    chunk1 = '%d%s' % (qty, chunk1)

            tempDateFlag       = self.dateFlag
            tempTimeFlag       = self.timeFlag
            sourceTime2, flag2 = self.parse(chunk1, sourceTime,postTime=postTime)
        else:
            return sourceTime, (flag1 and flag2)

        # if chunk1 is not a datetime and chunk2 is then do not use datetime
        # value returned by parsing chunk1
        if not (flag1 == False and flag2 == 0):
            sourceTime = sourceTime2
        else:
            self.timeFlag = tempTimeFlag
            self.dateFlag = tempDateFlag

        return sourceTime, (flag1 and flag2)
    def _evalString(self, datetimeString, sourceTime=None, postTime=None):
        """
        Calculate the datetime based on flags set by the L{parse()} routine

        Examples handled::
            RFC822, W3CDTF formatted dates
            HH:MM[:SS][ am/pm]
            MM/DD/YYYY
            DD MMMM YYYY

        @type  datetimeString: string
        @param datetimeString: text to try and parse as more "traditional"
                               date/time text
        @type  sourceTime:     struct_time
        @param sourceTime:     C{struct_time} value to use as the base

        @rtype:  datetime
        @return: calculated C{struct_time} value or current C{struct_time}
                 if not parsed
        """
        s   = datetimeString.strip()
        if postTime is not None:
            
            now=postTime
        else:
            now = time.localtime()

        # Given string date is a RFC822 date
        if sourceTime is None:
            sourceTime = _parse_date_rfc822(s)

            if sourceTime is not None:
                (yr, mth, dy, hr, mn, sec, wd, yd, isdst, _) = sourceTime
                self.dateFlag = 1

                if (hr != 0) and (mn != 0) and (sec != 0):
                    self.timeFlag = 2

                sourceTime = (yr, mth, dy, hr, mn, sec, wd, yd, isdst)

        # Given string date is a W3CDTF date
        if sourceTime is None:
            sourceTime = _parse_date_w3dtf(s)

            if sourceTime is not None:
                self.dateFlag = 1
                self.timeFlag = 2

        if sourceTime is None:
            s = s.lower()

        # Given string is in the format HH:MM(:SS)(am/pm)
        if self.meridianFlag:
            if sourceTime is None and postTime is None:
                (yr, mth, dy, hr, mn, sec, wd, yd, isdst) = now
            elif postTime is not None:
                (yr, mth, dy, hr, mn, sec, wd, yd, isdst) = postTime
            else:
                (yr, mth, dy, hr, mn, sec, wd, yd, isdst) = sourceTime

            m = self.ptc.CRE_TIMEHMS2.search(s)
            if m is not None:
                dt = s[:m.start('meridian')].strip()
                if len(dt) <= 2:
                    hr  = int(dt)
                    mn  = 0
                    sec = 0
                else:
                    hr, mn, sec = _extract_time(m)

                if hr == 24:
                    hr = 0

                sourceTime = (yr, mth, dy, hr, mn, sec, wd, yd, isdst)
                meridian   = m.group('meridian').lower()

                  # if 'am' found and hour is 12 - force hour to 0 (midnight)
                if (meridian in self.ptc.am) and hr == 12:
                    sourceTime = (yr, mth, dy, 0, mn, sec, wd, yd, isdst)

                  # if 'pm' found and hour < 12, add 12 to shift to evening
                if (meridian in self.ptc.pm) and hr < 12:
                    sourceTime = (yr, mth, dy, hr + 12, mn, sec, wd, yd, isdst)

              # invalid time
            if hr > 24 or mn > 59 or sec > 59:
                sourceTime    = now
                self.dateFlag = 0
                self.timeFlag = 0

            self.meridianFlag = False

          # Given string is in the format HH:MM(:SS)
        if self.timeStdFlag:
            if sourceTime is None and postTime is None:
                (yr, mth, dy, hr, mn, sec, wd, yd, isdst) = now
            elif postTime is not None:
                (yr, mth, dy, hr, mn, sec, wd, yd, isdst) = postTime
            else:
                (yr, mth, dy, hr, mn, sec, wd, yd, isdst) = sourceTime

            m = self.ptc.CRE_TIMEHMS.search(s)
            if m is not None:
                hr, mn, sec = _extract_time(m)
            if hr == 24:
                hr = 0

            if hr > 24 or mn > 59 or sec > 59:
                # invalid time
                sourceTime    = now
                self.dateFlag = 0
                self.timeFlag = 0
            else:
                sourceTime = (yr, mth, dy, hr, mn, sec, wd, yd, isdst)

            self.timeStdFlag = False

        # Given string is in the format 07/21/2006
        if self.dateStdFlag:
            sourceTime       = self.parseDate(s,postTime=postTime)
            self.dateStdFlag = False

        # Given string is in the format  "May 23rd, 2005"
        if self.dateStrFlag:
            sourceTime       = self.parseDateText(s,postTime=postTime)
            self.dateStrFlag = False

        # Given string is a weekday
        if self.weekdyFlag:
            (yr, mth, dy, hr, mn, sec, wd, yd, isdst) = now

            start = datetime.datetime(yr, mth, dy, hr, mn, sec)
            wkdy  = self.ptc.WeekdayOffsets[s]

            if wkdy > wd:
                qty = self._CalculateDOWDelta(wd, wkdy, 2,
                                              self.ptc.DOWParseStyle,
                                              self.ptc.CurrentDOWParseStyle)
            else:
                qty = self._CalculateDOWDelta(wd, wkdy, 2,
                                              self.ptc.DOWParseStyle,
                                              self.ptc.CurrentDOWParseStyle)

            target = start + datetime.timedelta(days=qty)
            wd     = wkdy

            sourceTime      = target.timetuple()
            self.weekdyFlag = False

        # Given string is a natural language time string like
        # lunch, midnight, etc
        if self.timeStrFlag:
            if s in self.ptc.re_values['now']:
                sourceTime = now
            else:
                sources = self.ptc.buildSources(sourceTime,postTime=postTime)

                if s in sources:
                    sourceTime = sources[s]
                else:
                    sourceTime    = now
                    self.dateFlag = 0
                    self.timeFlag = 0

            self.timeStrFlag = False

        # Given string is a natural language date string like today, tomorrow..
        if self.dayStrFlag:
            if sourceTime is None:
                sourceTime = now

            (yr, mth, dy, hr, mn, sec, wd, yd, isdst) = sourceTime

            if s in self.ptc.dayOffsets:
                offset = self.ptc.dayOffsets[s]
            else:
                offset = 0

            start      = datetime.datetime(yr, mth, dy, 9, 0, 0)
            target     = start + datetime.timedelta(days=offset)
            sourceTime = target.timetuple()

            self.dayStrFlag = False

        # Given string is a time string with units like "5 hrs 30 min"
        if self.unitsFlag:
            modifier = ''  # TODO

            if sourceTime is None:
                sourceTime = now

            m = self.ptc.CRE_UNITS.search(s)
            if m is not None:
                units    = m.group('units')
                quantity = s[:m.start('units')]

            sourceTime     = self._buildTime(sourceTime, quantity, modifier, units,postTime=postTime)
            self.unitsFlag = False

        # Given string is a time string with single char units like "5 h 30 m"
        if self.qunitsFlag:
            modifier = ''  # TODO

            if sourceTime is None:
                sourceTime = now

            m = self.ptc.CRE_QUNITS.search(s)
            if m is not None:
                units    = m.group('qunits')
                quantity = s[:m.start('qunits')]

            sourceTime      = self._buildTime(sourceTime, quantity, modifier, units,postTime=postTime)
            self.qunitsFlag = False

          # Given string does not match anything
        if sourceTime is None:
            sourceTime    = now
            self.dateFlag = 0
            self.timeFlag = 0

        return sourceTime
    
    def parse(self, datetimeString, sourceTime=None,postTime=None):
        """
        Splits the given C{datetimeString} into tokens, finds the regex
        patterns that match and then calculates a C{struct_time} value from
        the chunks.

        If C{sourceTime} is given then the C{struct_time} value will be
        calculated from that value, otherwise from the current date/time.

        If the C{datetimeString} is parsed and date/time value found then
        the second item of the returned tuple will be a flag to let you know
        what kind of C{struct_time} value is being returned::

            0 = not parsed at all
            1 = parsed as a C{date}
            2 = parsed as a C{time}
            3 = parsed as a C{datetime}

        @type  datetimeString: string
        @param datetimeString: date/time text to evaluate
        @type  sourceTime:     struct_time
        @param sourceTime:     C{struct_time} value to use as the base

        @rtype:  tuple
        @return: tuple of: modified C{sourceTime} and the result flag
        """

        if sourceTime:
            if isinstance(sourceTime, datetime.datetime):
                if _debug:
                    print 'coercing datetime to timetuple'
                sourceTime = sourceTime.timetuple()
            else:
                if not isinstance(sourceTime, time.struct_time) and \
                   not isinstance(sourceTime, tuple):
                    raise Exception('sourceTime is not a struct_time')

        s = datetimeString.strip().lower()
        parseStr  = ''
        totalTime = sourceTime

        if s == '' :
            if sourceTime is not None and postTime is None:
                if isinstance(sourceTime, time.struct_time):
                    sourceTime =  tuple([str(each) for each in sourceTime])
                return (sourceTime, self.dateFlag + self.timeFlag)
            elif postTime is not None:
                if isinstance(postTime, time.struct_time):
                    postTime = tuple([str(each) for each in postTime])
                return(postTime,0)
            else:
                if isinstance(time.localtime(), time.struct_time):
                    return tuple([str(each) for each in time.localtime()])
        
                return (time.localtime(), 0)

        self.timeFlag = 0
        self.dateFlag = 0

        while len(s) > 0:
            flag   = False
            chunk1 = ''
            chunk2 = ''

            if _debug:
                print 'parse (top of loop): [%s][%s]' % (s, parseStr)

            if parseStr == '':
                # Modifier like next\prev..
                m = self.ptc.CRE_MODIFIER.search(s)
                if m is not None:
                    self.modifierFlag = True
                    if (m.group('modifier') != s):
                        # capture remaining string
                        parseStr = m.group('modifier')
                        chunk1   = s[:m.start('modifier')].strip()
                        chunk2   = s[m.end('modifier'):].strip()
                        flag     = True
                    else:
                        parseStr = s

            if parseStr == '':
                # Modifier like from\after\prior..
                m = self.ptc.CRE_MODIFIER2.search(s)
                if m is not None:
                    self.modifier2Flag = True
                    if (m.group('modifier') != s):
                        # capture remaining string
                        parseStr = m.group('modifier')
                        chunk1   = s[:m.start('modifier')].strip()
                        chunk2   = s[m.end('modifier'):].strip()
                        flag     = True
                    else:
                        parseStr = s

            if parseStr == '':
                valid_date = False
                for match in self.ptc.CRE_DATE3.finditer(s):
                    # to prevent "HH:MM(:SS) time strings" expressions from triggering
                    # this regex, we checks if the month field exists in the searched 
                    # expression, if it doesn't exist, the date field is not valid
                    if match.group('mthname'):
                        m = self.ptc.CRE_DATE3.search(s, match.start())
                        valid_date = True
                        break

                # String date format
                if valid_date:
                    self.dateStrFlag = True
                    self.dateFlag    = 1
                    if (m.group('date') != s):
                        # capture remaining string
                        parseStr = m.group('date')
                        chunk1   = s[:m.start('date')]
                        chunk2   = s[m.end('date'):]
                        s        = '%s %s' % (chunk1, chunk2)
                        flag     = True
                    else:
                        parseStr = s

            if parseStr == '':
                # Standard date format
                m = self.ptc.CRE_DATE.search(s)
                if m is not None:
                    self.dateStdFlag = True
                    self.dateFlag    = 1
                    if (m.group('date') != s):
                        # capture remaining string
                        parseStr = m.group('date')
                        chunk1   = s[:m.start('date')]
                        chunk2   = s[m.end('date'):]
                        s        = '%s %s' % (chunk1, chunk2)
                        flag     = True
                    else:
                        parseStr = s

            if parseStr == '':
                # Natural language day strings
                m = self.ptc.CRE_DAY.search(s)
                if m is not None:
                    self.dayStrFlag = True
                    self.dateFlag   = 1
                    if (m.group('day') != s):
                        # capture remaining string
                        parseStr = m.group('day')
                        chunk1   = s[:m.start('day')]
                        chunk2   = s[m.end('day'):]
                        s        = '%s %s' % (chunk1, chunk2)
                        flag     = True
                    else:
                        parseStr = s

            if parseStr == '':
                # Quantity + Units
                m = self.ptc.CRE_UNITS.search(s)
                if m is not None:
                    self.unitsFlag = True
                    if (m.group('qty') != s):
                        # capture remaining string
                        parseStr = m.group('qty')
                        chunk1   = s[:m.start('qty')].strip()
                        chunk2   = s[m.end('qty'):].strip()

                        if chunk1[-1:] == '-':
                            parseStr = '-%s' % parseStr
                            chunk1   = chunk1[:-1]

                        s    = '%s %s' % (chunk1, chunk2)
                        flag = True
                    else:
                        parseStr = s

            if parseStr == '':
                # Quantity + Units
                m = self.ptc.CRE_QUNITS.search(s)
                if m is not None:
                    self.qunitsFlag = True

                    if (m.group('qty') != s):
                        # capture remaining string
                        parseStr = m.group('qty')
                        chunk1   = s[:m.start('qty')].strip()
                        chunk2   = s[m.end('qty'):].strip()

                        if chunk1[-1:] == '-':
                            parseStr = '-%s' % parseStr
                            chunk1   = chunk1[:-1]

                        s    = '%s %s' % (chunk1, chunk2)
                        flag = True
                    else:
                        parseStr = s 

            if parseStr == '':
                # Weekday
                m = self.ptc.CRE_WEEKDAY.search(s)
                if m is not None:
                    gv = m.group('weekday')
                    if s not in self.ptc.dayOffsets:
                        self.weekdyFlag = True
                        self.dateFlag   = 1
                        if (gv != s):
                            # capture remaining string
                            parseStr = gv
                            chunk1   = s[:m.start('weekday')]
                            chunk2   = s[m.end('weekday'):]
                            s        = '%s %s' % (chunk1, chunk2)
                            flag     = True
                        else:
                            parseStr = s

            if parseStr == '':
                # Natural language time strings
                m = self.ptc.CRE_TIME.search(s)
                if m is not None:
                    self.timeStrFlag = True
                    self.timeFlag    = 2
                    if (m.group('time') != s):
                        # capture remaining string
                        parseStr = m.group('time')
                        chunk1   = s[:m.start('time')]
                        chunk2   = s[m.end('time'):]
                        s        = '%s %s' % (chunk1, chunk2)
                        flag     = True
                    else:
                        parseStr = s

            if parseStr == '':
                # HH:MM(:SS) am/pm time strings
                m = self.ptc.CRE_TIMEHMS2.search(s)
                if m is not None:
                    self.meridianFlag = True
                    self.timeFlag     = 2
                    if m.group('minutes') is not None:
                        if m.group('seconds') is not None:
                            parseStr = '%s:%s:%s %s' % (m.group('hours'),
                                                        m.group('minutes'),
                                                        m.group('seconds'),
                                                        m.group('meridian'))
                        else:
                            parseStr = '%s:%s %s' % (m.group('hours'),
                                                     m.group('minutes'),
                                                     m.group('meridian'))
                    else:
                        parseStr = '%s %s' % (m.group('hours'),
                                              m.group('meridian'))

                    chunk1 = s[:m.start('hours')]
                    chunk2 = s[m.end('meridian'):]

                    s    = '%s %s' % (chunk1, chunk2)
                    flag = True

            if parseStr == '':
                # HH:MM(:SS) time strings
                m = self.ptc.CRE_TIMEHMS.search(s)
                if m is not None:
                    self.timeStdFlag = True
                    self.timeFlag    = 2
                    if m.group('seconds') is not None:
                        parseStr = '%s:%s:%s' % (m.group('hours'),
                                                 m.group('minutes'),
                                                 m.group('seconds'))
                        chunk1   = s[:m.start('hours')]
                        chunk2   = s[m.end('seconds'):]
                    else:
                        parseStr = '%s:%s' % (m.group('hours'),
                                              m.group('minutes'))
                        chunk1   = s[:m.start('hours')]
                        chunk2   = s[m.end('minutes'):]

                    s    = '%s %s' % (chunk1, chunk2)
                    flag = True

            # if string does not match any regex, empty string to
            # come out of the while loop
            if not flag:
                s = ''

            if _debug:
                print 'parse (bottom) [%s][%s][%s][%s]' % (s, parseStr, chunk1, chunk2)
                print 'weekday %s, dateStd %s, dateStr %s, time %s, timeStr %s, meridian %s' % \
                       (self.weekdyFlag, self.dateStdFlag, self.dateStrFlag, self.timeStdFlag, self.timeStrFlag, self.meridianFlag)
                print 'dayStr %s, modifier %s, modifier2 %s, units %s, qunits %s' % \
                       (self.dayStrFlag, self.modifierFlag, self.modifier2Flag, self.unitsFlag, self.qunitsFlag)

            # evaluate the matched string
            if parseStr != '':
                if self.modifierFlag == True:
                    t, totalTime = self._evalModifier(parseStr, chunk1, chunk2, totalTime,postTime=postTime)
                    # t is the unparsed part of the chunks.
                    # If it is not date/time, return current
                    # totalTime as it is; else return the output
                    # after parsing t.
                    if (t != '') and (t != None):
                        tempDateFlag       = self.dateFlag
                        tempTimeFlag       = self.timeFlag
                        (totalTime2, flag) = self.parse(t, totalTime,postTime=postTime)

                        if flag == 0 and totalTime is not None and postTime is  None:
                            self.timeFlag = tempTimeFlag
                            self.dateFlag = tempDateFlag
                            #if isinstance(totalTime, time.struct_time):
                             #   totalTime = tuple([each for each in totalTime])
                            if isinstance(totalTime,time.struct_time):
                                totalTime = tuple([str(each) for each in totalTime])
                            return (totalTime, self.dateFlag + self.timeFlag)
                        else:
                           #if isinstance(totalTime, time.struct_time):
                            #    totalTime2 = tuple([each for each in totalTime])
                            if isinstance(totalTime2, time.struct_time):
                                #print 'Hi'
                                totalTime2 = tuple([str(each) for each in totalTime2])
                            return (totalTime2, self.dateFlag + self.timeFlag)

                elif self.modifier2Flag == True:
                    totalTime, invalidFlag = self._evalModifier2(parseStr, chunk1, chunk2, totalTime,postTime=postTime)

                    if invalidFlag == True:
                        self.dateFlag = 0
                        self.timeFlag = 0

                else:
                    totalTime = self._evalString(parseStr, totalTime,postTime=postTime)
                    parseStr  = ''

        # String is not parsed at all
        if totalTime is None or totalTime == sourceTime and postTime is None:
            totalTime     = time.localtime()
            self.dateFlag = 0
            self.timeFlag = 0
        elif postTime is not None:
            totalTime = postTime
        if isinstance(totalTime, time.struct_time):
            totalTime = tuple([str(each) for each in totalTime])
        return (totalTime, self.dateFlag + self.timeFlag)


    
