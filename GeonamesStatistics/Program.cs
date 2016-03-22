using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace GeonamesStatistics
{
    class Program
    {
        static void Main(string[] args)
        {
            int _outputLineNum = 0;

            string geonamesDirectory = args.Length >= 1 ? args[0] : null;
            if (string.IsNullOrWhiteSpace(geonamesDirectory) || !Directory.Exists(geonamesDirectory))
            {
                Console.WriteLine("Please enter the Geonames directory path...");
                _outputLineNum++;
                geonamesDirectory = Console.ReadLine();
                _outputLineNum++;
                while (string.IsNullOrWhiteSpace(geonamesDirectory) && !Directory.Exists(geonamesDirectory))
                {
                    Console.WriteLine("Invalid Directory entered.");
                    _outputLineNum++;
                    Console.WriteLine("Please enter a valid Geonames Directory path...");
                    _outputLineNum++;
                    geonamesDirectory = Console.ReadLine();
                    _outputLineNum++;
                }
            }
            Console.WriteLine();

            string featureCodeFile = args.Length >= 2 ? args[1] : null;
            if (string.IsNullOrWhiteSpace(featureCodeFile) || !File.Exists(featureCodeFile))
            {
                Console.WriteLine("Please enter the FeatureCode file path...");
                _outputLineNum++;
                featureCodeFile = Console.ReadLine();
                _outputLineNum++;
                while (string.IsNullOrWhiteSpace(featureCodeFile) && !File.Exists(featureCodeFile))
                {
                    Console.WriteLine("Invalid file path entered.");
                    _outputLineNum++;
                    Console.WriteLine("Please enter a valid FeatureCode file path...");
                    _outputLineNum++;
                    featureCodeFile = Console.ReadLine();
                    _outputLineNum++;
                }
            }
            Console.WriteLine();

            string outputFile = args.Length >= 3 ? args[2] : null;
            if (string.IsNullOrWhiteSpace(outputFile) || !Uri.IsWellFormedUriString(outputFile, UriKind.Absolute))
            {
                Console.WriteLine("Please enter the output file path (enter nothing for none)...");
                _outputLineNum++;
                outputFile = Console.ReadLine();
                _outputLineNum++;
                while (!string.IsNullOrWhiteSpace(outputFile) && !Directory.Exists(geonamesDirectory))
                {
                    Console.WriteLine("Invalid path entered.");
                    _outputLineNum++;
                    Console.WriteLine("Please enter a valid output file path (or nothing for just console output)...");
                    _outputLineNum++;
                    geonamesDirectory = Console.ReadLine();
                    _outputLineNum++;
                }
            }
            Console.WriteLine();

            Console.WriteLine("Calculating Geonames Statistics, please wait...");
            _outputLineNum++;

            object _lockObject = new object();
            bool _firstFile = true;

            int _numLines = 0;
            int _numFiles = 0;

            double _avrgLines = 0;
            int _minLines = 0;
            int _maxLines = 0;

            double _totalLines = 0;
            int _totalFiles = 0;

            int _maxIdLength = 0;
            int _maxNameLength = 0;
            int _maxAsciiNameLength = 0;
            int _maxAlternateNamesLength = 0;
            double _maxLatitudeLength = 0D;
            double _maxLongitudeLength = 0D;
            int _maxFeatureClassLength = 0;
            int _maxFeatureCodeLength = 0;
            int _maxCountryCodeLength = 0;
            int _maxCountryCode2Length = 0;
            long _maxPopulationLength = 0L;
            int _maxElevationLength = 0;
            int _maxAdmin1CodeLength = 0;
            int _maxAdmin2CodeLength = 0;
            int _maxAdmin3CodeLength = 0;
            int _maxAdmin4CodeLength = 0;
            int _maxGtopo30Length = 0;
            int _maxTimeZoneNameLength = 0;

            bool _idIsNull = false;
            bool _nameIsNull = false;
            bool _asciiNameIsNull = false;
            bool _alternateNamesIsNull = false;
            bool _latitudeIsNull = false;
            bool _longitudeIsNull = false;
            bool _featureClassIsNull = false;
            bool _featureCodeIsNull = false;
            bool _countryCodeIsNull = false;
            bool _countryCode2IsNull = false;
            bool _admin1CodeIsNull = false;
            bool _admin2CodeIsNull = false;
            bool _admin3CodeIsNull = false;
            bool _admin4CodeIsNull = false;
            bool _populationIsNull = false;
            bool _elevationIsNull = false;
            bool _gtopo30IsNull = false;
            bool _timeZoneNameIsNull = false;

            // Feature Code fields
            int _totalFeatureCodeLines = 0;

            int _maxFCCodeLength = 0;
            int _maxFCNameLength = 0;
            int _maxFCDescriptionLength = 0;

            bool _fcCodeIsNull = false;
            bool _fcNameIsNull = false;
            bool _fcDescriptionIsNull = false;

            Regex tabGex = new Regex("\t", RegexOptions.Compiled);

            Task fcTask = new Task(
                ((Action)(()
                    =>
                    {
                        using (StreamReader input = new StreamReader(featureCodeFile))
                        {
                            while (!input.EndOfStream)
                            {
                                _totalFeatureCodeLines++;
                                string[] tokens = tabGex.Split(input.ReadLine());
                                int fcCodeLength = tokens[0].Length;
                                if (fcCodeLength > _maxFCCodeLength)
                                {
                                    _maxFCCodeLength = fcCodeLength;
                                }
                                if (fcCodeLength == 0 && !_fcCodeIsNull)
                                {
                                    _fcCodeIsNull = true;
                                }

                                int fcNameLength = tokens[1].Length;
                                if (fcNameLength > _maxFCNameLength)
                                {
                                    _maxFCNameLength = fcNameLength;
                                }
                                if (fcNameLength == 0 && !_fcNameIsNull)
                                {
                                    _fcNameIsNull = true;
                                }

                                int fcDescriptionLength = tokens[2].Length;
                                if (fcDescriptionLength > _maxFCDescriptionLength)
                                {
                                    _maxFCDescriptionLength = fcDescriptionLength;
                                }
                                if (fcDescriptionLength == 0 && !_fcDescriptionIsNull)
                                {
                                    _fcDescriptionIsNull = true;
                                }
                            }
                        }
                    })));
            fcTask.Start();

            DirectoryInfo directory = new DirectoryInfo(geonamesDirectory);

            FileInfo[] files = directory.GetFiles();
            _totalFiles = files.Length;

            Console.Write("Parsing file # ");

            int _cursorLeft = Console.CursorLeft;

            Parallel.ForEach<FileInfo>(
                files,
                new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                (i, a) =>
                {
                    lock (_lockObject)
                    {
                        Console.SetCursorPosition(_cursorLeft, Console.CursorTop);
                        Console.Write(++_numFiles);
                        Console.Write("/");
                        Console.Write(_totalFiles);
                    }

                    using (StreamReader input = new StreamReader(i.FullName))
                    {
                        while (!input.EndOfStream)
                        {
                            string line = input.ReadLine();

                            Interlocked.Increment(ref _numLines);

                            string[] tokens = tabGex.Split(line);

                            int idLength = -1;
                            if (!int.TryParse(tokens[0], out idLength))
                            {
                                _idIsNull = true;
                            }
                            else
                            {
                                if (idLength > _maxIdLength)
                                {
                                    _maxIdLength = idLength;
                                }
                            }

                            int nameLength = tokens[1].Length;
                            if (nameLength > _maxNameLength)
                            {
                                _maxNameLength = nameLength;
                            }
                            if (nameLength == 0 && !_nameIsNull)
                            {
                                _nameIsNull = true;
                            }

                            int asciiNameLength = tokens[2].Length;
                            if (asciiNameLength > _maxAsciiNameLength)
                            {
                                _maxAsciiNameLength = asciiNameLength;
                            }
                            if (asciiNameLength == 0 && !_asciiNameIsNull)
                            {
                                _asciiNameIsNull = true;
                            }

                            int alternateNamesLength = tokens[3].Length;
                            if (alternateNamesLength > _maxAlternateNamesLength)
                            {
                                _maxAlternateNamesLength = alternateNamesLength;
                            }
                            if (alternateNamesLength == 0 && !_alternateNamesIsNull)
                            {
                                _alternateNamesIsNull = true;
                            }

                            double latitudeLength = -1D;
                            if (!double.TryParse(tokens[4], out latitudeLength))
                            {
                                _latitudeIsNull = true;
                            }
                            else
                            {
                                if (latitudeLength > _maxLatitudeLength)
                                {
                                    _maxLatitudeLength = latitudeLength;
                                }
                            }

                            double longitudeLength = -1D;
                            if (!double.TryParse(tokens[5], out longitudeLength))
                            {
                                _longitudeIsNull = true;
                            }
                            else
                            {
                                if (longitudeLength > _maxLongitudeLength)
                                {
                                    _maxLongitudeLength = longitudeLength;
                                }
                            }

                            int featureClassLength = tokens[6].Length;
                            if (featureClassLength > _maxFeatureClassLength)
                            {
                                _maxFeatureClassLength = featureClassLength;
                            }
                            if (featureClassLength == 0 && !_featureClassIsNull)
                            {
                                _featureClassIsNull = true;
                            }

                            int featureCodeLength = tokens[7].Length;
                            if (featureCodeLength > _maxFeatureCodeLength)
                            {
                                _maxFeatureCodeLength = featureCodeLength;
                            }
                            if (featureCodeLength == 0 && !_featureCodeIsNull)
                            {
                                _featureCodeIsNull = true;
                            }

                            int countryCodeLength = tokens[8].Length;
                            if (countryCodeLength > _maxCountryCodeLength)
                            {
                                _maxCountryCodeLength = countryCodeLength;
                            }
                            if (countryCodeLength == 0 && !_countryCodeIsNull)
                            {
                                _countryCodeIsNull = true;
                            }

                            int countryCode2Length = tokens[9].Length;
                            if (countryCode2Length > _maxCountryCode2Length)
                            {
                                _maxCountryCode2Length = countryCode2Length;
                            }
                            if (countryCode2Length == 0 && !_countryCode2IsNull)
                            {
                                _countryCode2IsNull = true;
                            }

                            int admin1CodeLength = tokens[10].Length;
                            if (admin1CodeLength > _maxAdmin1CodeLength)
                            {
                                _maxAdmin1CodeLength = admin1CodeLength;
                            }
                            if (admin1CodeLength == 0 && !_admin1CodeIsNull)
                            {
                                _admin1CodeIsNull = true;
                            }

                            int admin2CodeLength = tokens[11].Length;
                            if (admin2CodeLength > _maxAdmin2CodeLength)
                            {
                                _maxAdmin2CodeLength = admin2CodeLength;
                            }
                            if (admin2CodeLength == 0 && !_admin2CodeIsNull)
                            {
                                _admin2CodeIsNull = true;
                            }

                            int admin3CodeLength = tokens[12].Length;
                            if (admin3CodeLength > _maxAdmin3CodeLength)
                            {
                                _maxAdmin3CodeLength = admin3CodeLength;
                            }
                            if (admin3CodeLength == 0 && !_admin3CodeIsNull)
                            {
                                _admin3CodeIsNull = true;
                            }

                            int admin4CodeLength = tokens[13].Length;
                            if (admin4CodeLength > _maxAdmin4CodeLength)
                            {
                                _maxAdmin4CodeLength = admin4CodeLength;
                            }
                            if (admin4CodeLength == 0 && !_admin4CodeIsNull)
                            {
                                _admin4CodeIsNull = true;
                            }

                            long populationLength = -1L;
                            if (!long.TryParse(tokens[14], out populationLength))
                            {
                                _populationIsNull = true;
                            }
                            else
                            {
                                if (populationLength > _maxPopulationLength)
                                {
                                    _maxPopulationLength = populationLength;
                                }
                            }

                            int elevationLength = -1;
                            if (!int.TryParse(tokens[15], out elevationLength))
                            {
                                _elevationIsNull = true;
                            }
                            else
                            {
                                if (elevationLength > _maxElevationLength)
                                {
                                    _maxElevationLength = elevationLength;
                                }
                            }

                            int gtopo30Length = -1;
                            if (!int.TryParse(tokens[16], out gtopo30Length))
                            {
                                _gtopo30IsNull = true;
                            }
                            else
                            {
                                if (gtopo30Length > _maxGtopo30Length)
                                {
                                    _maxGtopo30Length = gtopo30Length;
                                }
                            }

                            int timeZoneNameLength = tokens[17].Length;
                            if (timeZoneNameLength > _maxTimeZoneNameLength)
                            {
                                _maxTimeZoneNameLength = timeZoneNameLength;
                            }
                            if (timeZoneNameLength == 0 && !_timeZoneNameIsNull)
                            {
                                _timeZoneNameIsNull = true;
                            }
                        }

                        lock (_lockObject)
                        {

                            if (_firstFile)
                            {
                                _firstFile = false;
                                _minLines = _numLines;
                            }

                            if (_numLines < _minLines)
                            {
                                _minLines = _totalFiles;
                            }

                            if (_numLines > _maxLines)
                            {
                                _maxLines = _numLines;
                            }

                            _totalLines += _numLines;
                        }
                    }
                });

            _avrgLines = _totalLines / (double)_totalFiles;

            Task.WaitAll(fcTask);

            Console.WriteLine("\nDone!");

            StringBuilder sb = new StringBuilder("Results:\n");
            sb.AppendLine("There are " + _totalLines + " total lines.");
            sb.AppendLine("There are " + _totalFiles + " files.");
            sb.AppendLine("The File with the least lines had " + _minLines);
            sb.AppendLine("The File with the most lines had " + _maxLines);
            sb.AppendLine("The average number of lines is " + _avrgLines);
            sb.AppendLine(string.Empty);
            sb.AppendLine(string.Empty);
            sb.AppendLine("Property:       Max Length = value. Is ever null = true/false\n");
            sb.AppendLine("ID:             Max Length = " + _maxIdLength + ". Is ever null = " + _idIsNull);
            sb.AppendLine("Name:           Max Length = " + _maxNameLength + ". Is ever null = " + _nameIsNull);
            sb.AppendLine("AsciiName:      Max Length = " + _maxAsciiNameLength + ". Is ever null = " + _asciiNameIsNull);
            sb.AppendLine("AlternateNames: Max Length = " + _maxAlternateNamesLength + ". Is ever null = " + _alternateNamesIsNull);
            sb.AppendLine("Latitude:       Max Length = " + _maxLatitudeLength + ". Is ever null = " + _latitudeIsNull);
            sb.AppendLine("Longitude:      Max Length = " + _maxLongitudeLength + ". Is ever null = " + _longitudeIsNull);
            sb.AppendLine("FeatureClass:   Max Length = " + _maxFeatureClassLength + ". Is ever null = " + _featureClassIsNull);
            sb.AppendLine("FeatureCode:    Max Length = " + _maxFeatureCodeLength + ". Is ever null = " + _featureCodeIsNull);
            sb.AppendLine("CountryCode:    Max Length = " + _maxCountryCodeLength + ". Is ever null = " + _countryCodeIsNull);
            sb.AppendLine("CountryCode2:   Max Length = " + _maxCountryCode2Length + ". Is ever null = " + _countryCode2IsNull);
            sb.AppendLine("Population:     Max Length = " + _maxPopulationLength + ". Is ever null = " + _populationIsNull);
            sb.AppendLine("Elevation:      Max Length = " + _maxElevationLength + ". Is ever null = " + _elevationIsNull);
            sb.AppendLine("Admin1Code:     Max Length = " + _maxAdmin1CodeLength + ". Is ever null = " + _admin1CodeIsNull);
            sb.AppendLine("Admin2Code:     Max Length = " + _maxAdmin2CodeLength + ". Is ever null = " + _admin2CodeIsNull);
            sb.AppendLine("Admin3Code:     Max Length = " + _maxAdmin3CodeLength + ". Is ever null = " + _admin3CodeIsNull);
            sb.AppendLine("Admin4Code:     Max Length = " + _maxAdmin4CodeLength + ". Is ever null = " + _admin4CodeIsNull);
            sb.AppendLine("Gtopo30:        Max Length = " + _maxGtopo30Length + ". Is ever null = " + _gtopo30IsNull);
            sb.AppendLine("TimeZoneName:   Max Length = " + _maxTimeZoneNameLength + ". Is ever null = " + _timeZoneNameIsNull);
            sb.AppendLine(string.Empty);
            sb.AppendLine(string.Empty);
            // Feature Code results
            sb.AppendLine("Feature Code results:");
            sb.AppendLine("There are " + _totalFeatureCodeLines + " total lines.");
            sb.AppendLine("Property:    Max Length = value. Is ever null = true/false\n");
            sb.AppendLine("Code:        Max Length = " + _maxFCCodeLength + ". Is ever null = " + _fcCodeIsNull);
            sb.AppendLine("Name:        Max Length = " + _maxFCNameLength + ". Is ever null = " + _fcNameIsNull);
            sb.AppendLine("Description: Max Length = " + _maxFCDescriptionLength + ". Is ever null = " + _fcDescriptionIsNull);
            sb.AppendLine(string.Empty);

            string result = sb.ToString();
            Console.WriteLine(result);

            if (null != outputFile)
            {
                using (StreamWriter output = new StreamWriter(outputFile, false))
                {
                    output.WriteLine(result);
                }
                Console.WriteLine("Output successfully written.");
            }
            Console.WriteLine("Press any key to terminate...");
            Console.ReadLine();
        }
    }
}
