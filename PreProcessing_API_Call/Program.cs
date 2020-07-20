using Deedle;
using MathNet.Filtering;
using MathNet.Numerics.Statistics;
using RestSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Preprocessing_API_Call
{
    class API_CALL
    {

        public static string CSharpPythonRestfulApiSimpleTest(string WebAPI, out string exceptionMessage, String json)
        {
            exceptionMessage = string.Empty;
            string webResponse = string.Empty;
            try
            {
                var client = new RestClient(WebAPI);

                var request = new RestRequest();
                request.Method = Method.POST;
                request.AddHeader("Content-type", "application/json");
                //request.RequestFormat = DataFormat.Json;
                request.AddJsonBody(
                    json
                ); // uses JsonSerializer

                var response = client.Post(request);
                Console.WriteLine(response.Content);

                //Console.WriteLine(count);
                //HTTP status code 200-success
                Console.WriteLine(response.StatusCode);
            }
            catch (Exception ex)
            {
                exceptionMessage = $"An error occurred. {ex.Message}";
            }
            return webResponse;
        }

        public static List<String> Jsonbody(Frame<int, String> df)
        {
            List<String> json_list = new List<String>();
            foreach (ObjectSeries<String> rows in df.Rows.Values)
            {
                String json_str = "{ ";
                foreach (var cols in rows.Keys)
                {
                    if (cols == rows.Keys.Last())
                    {

                        json_str += $"\"{cols}\" : {rows[cols]}";
                    }
                    else
                    {
                        json_str += $"\"{cols}\" : {rows[cols]}, ";
                    }
                }
                json_str += "}";


                json_list.Add(json_str);

            }

            return json_list;
        }
    }

    class PreProcessing
    {
        static void Main(string[] args)
        {
            int seg_size = 1280;
            Double cut_off_value = 3.0;
            //string[] stat_list = { "mean", "max", "min", "var", "std", "median", "skewness", "kurtosis" };

            var file_list = Directory.GetFiles(@"./test_set/", "*.csv", SearchOption.AllDirectories);

            foreach (string files in file_list)
            {
                Console.WriteLine(files);
                var data = Frame.ReadCsv(files, true);
                var condition_df = data.Clone();

                var df = Frame.CreateEmpty<int, String>();
                String[] DropColumn_list = { "Ap", "Ae", "RPM", "Feedrate", "Machine", "Cutting Direction", "Material", "Cutting Type", "Flute", "Time" };

                foreach (String drop_col in DropColumn_list)
                {
                    data.DropColumn($"{drop_col}");
                }
                //"Machine", "Cutting_Direction", "Material", "Cutting_Type",
                String[] condition_list = { "Ap", "Ae", "RPM", "Feedrate", "Flute" };

                foreach (String condition_col in condition_list)
                {
                    df[$"{condition_col}"] = condition_df[$"{condition_col}"].GroupBy(kvp => kvp.Key / seg_size).Select(kvp => kvp.Value.FirstValue());
                }

                //구간 별 (마지막 데이터 시간 - 첫 데이터 시간) 소모 시간 계산
                //df["Time"] = condition_df["Time"].GroupBy( kvp => kvp.Key/seg_size).Select(kvp => kvp.Value.Values.First() - kvp.Value.Values.Last());


                //<LPF 적용 부분>
                
                Double cut_off = (condition_df["RPM"][0] / 60.0) * 2 * cut_off_value;
                int sampling_freq = 12800;
                String[] LPF_list = {"X FT", "Y FT", "Z FT", "S R AMP", "S S AMP", "S T AMP", "X R AMP", "X S AMP", "X T AMP",
                            "Y R AMP", "Y S AMP", "Y T AMP", "Z R AMP", "Z S AMP", "Z T AMP", "X1 AMP", "Y1 AMP", "Z1 AMP" };

                var low_pass_filter = OnlineFilter.CreateLowpass(ImpulseResponse.Finite, sampling_freq, cut_off, 5);
                var low_pass_filter_amp = OnlineFilter.CreateLowpass(ImpulseResponse.Finite, sampling_freq, 66, 5);

                foreach (String col_name in data.Columns.Keys) 
                {
                    if (LPF_list.Contains(col_name))
                    {
                        if (col_name.EndsWith("AMP"))
                        {
                            data[$"{col_name}"] = low_pass_filter_amp.ProcessSamples(data[$"{col_name}"].Values.ToArray()).ToOrdinalSeries();
                        }
                        else
                        {
                            data[$"{col_name}"] = low_pass_filter.ProcessSamples(data[$"{col_name}"].Values.ToArray()).ToOrdinalSeries();
                        }
                    }
                }
                //</LFP 적용 부분>
                

                //<RMS 계산>
                try
                {
                    data["RMS_XY_FT"] = Series<int, String>.Sqrt(Series<int, String>.Pow(data["X FT"], 2) + Series<int, String>.Pow(data["Y FT"], 2));
                }
                catch (Exception)
                {
                    throw;
                }
                try
                {
                    data["RMS_XYZ_FT"] = Series<int, String>.Sqrt(Series<int, String>.Pow(data["X FT"], 2) + Series<int, String>.Pow(data["Y FT"], 2) + Series<int, String>.Pow(data["Z FT"], 2));
                }
                catch (Exception)
                {
                    throw;
                }
                try
                {
                    data["RMS_Spindle_V"] = Series<int, String>.Sqrt(Series<int, String>.Pow(data["R V"], 2) + Series<int, String>.Pow(data["S V"], 2) + Series<int, String>.Pow(data["T V"], 2));
                }
                catch (Exception)
                {
                    throw;
                }
                try
                {
                    data["RMS_Spindle_AMP"] = Series<int, String>.Sqrt(Series<int, String>.Pow(data["S R AMP"], 2) + Series<int, String>.Pow(data["S S AMP"], 2) + Series<int, String>.Pow(data["S T AMP"], 2));
                }
                catch (Exception)
                {
                    throw;
                }
                try
                {
                    data["RMS_X_AMP"] = Series<int, String>.Sqrt(Series<int, String>.Pow(data["X R AMP"], 2) + Series<int, String>.Pow(data["X S AMP"], 2) + Series<int, String>.Pow(data["X T AMP"], 2));
                }
                catch (Exception)
                {
                    throw;
                }
                try
                {
                    data["RMS_Y_AMP"] = Series<int, String>.Sqrt(Series<int, String>.Pow(data["Y R AMP"], 2) + Series<int, String>.Pow(data["Y S AMP"], 2) + Series<int, String>.Pow(data["Y T AMP"], 2));
                }
                catch (Exception)
                {
                    throw;
                }
                try
                {
                    data["RMS_Z_AMP"] = Series<int, String>.Sqrt(Series<int, String>.Pow(data["Z R AMP"], 2) + Series<int, String>.Pow(data["Z S AMP"], 2) + Series<int, String>.Pow(data["Z T AMP"], 2));
                }
                catch (Exception)
                {
                    throw;
                }
                //</RMS 계산>


                //<통계 값 계산>
                foreach (String col_name in data.ColumnKeys)
                {
                    df[$"mean_{col_name}"] = data[$"{col_name}"].GroupBy(kvp => kvp.Key / seg_size).Select(kvp => kvp.Value.Values.Mean());
                    df[$"max_{col_name}"] = data[$"{col_name}"].GroupBy(kvp => kvp.Key / seg_size).Select(kvp => kvp.Value.Values.Max());
                    df[$"min_{col_name}"] = data[$"{col_name}"].GroupBy(kvp => kvp.Key / seg_size).Select(kvp => kvp.Value.Values.Min());
                    df[$"var_{col_name}"] = data[$"{col_name}"].GroupBy(kvp => kvp.Key / seg_size).Select(kvp => kvp.Value.Values.Variance());
                    df[$"std_{col_name}"] = data[$"{col_name}"].GroupBy(kvp => kvp.Key / seg_size).Select(kvp => kvp.Value.Values.StandardDeviation());
                    df[$"median_{col_name}"] = data[$"{col_name}"].GroupBy(kvp => kvp.Key / seg_size).Select(kvp => kvp.Value.Values.Median());
                    df[$"skew_{col_name}"] = data[$"{col_name}"].GroupBy(kvp => kvp.Key / seg_size).Select(kvp => kvp.Value.Values.Skewness());
                    df[$"kur_{col_name}"] = data[$"{col_name}"].GroupBy(kvp => kvp.Key / seg_size).Select(kvp => kvp.Value.Values.Kurtosis());
                }
                df["max_of_max_min_X_FT"] = get_bigger(df["max_X FT"], Series<int, String>.Abs(df["min_X FT"]));
                df["max_of_max_min_Y_FT"] = get_bigger(df["max_Y FT"], Series<int, String>.Abs(df["min_Y FT"]));
                df["max_of_max_min_Z_FT"] = get_bigger(df["max_Z FT"], Series<int, String>.Abs(df["min_Z FT"]));
                df["max_of_max_min_RMS_XY_FT"] = get_bigger(df["max_RMS_XY_FT"], Series<int, String>.Abs(df["min_RMS_XY_FT"]));
                df["max_of_max_min_RMS_XYZ_FT"] = get_bigger(df["max_RMS_XYZ_FT"], Series<int, String>.Abs(df["min_RMS_XYZ_FT"]));

                df["max_min_X_FT"] = df["max_X FT"] - df["min_X FT"];
                df["max_min_Y_FT"] = df["max_Y FT"] - df["min_Y FT"];
                df["max_min_Z_FT"] = df["max_Z FT"] - df["min_Z FT"];
                df["max_min_RMS_XY_FT"] = df["max_RMS_XY_FT"] - df["min_RMS_XY_FT"];
                df["max_min_RMS_XYZ_FT"] = df["max_RMS_XYZ_FT"] - df["min_RMS_XYZ_FT"];

                df.SaveCsv($"D:/C#test_result/{files}");
                //</통계 값 계산>
                
               /* // df to json
                List<String> json_list = API_CALL.Jsonbody(df);

                //<API CALL>
                string WebAPI, exceptionMessage, webResponse;
                WebAPI = "http://10.250.105.230/test_pkl";
                //WebAPI = "http://localhost/test2";
                exceptionMessage = string.Empty;


                foreach (String json in json_list)
                {
                    Stopwatch stopwatch = new Stopwatch();

                    stopwatch.Start();
                    webResponse = API_CALL.CSharpPythonRestfulApiSimpleTest(WebAPI, out exceptionMessage, json);
                    stopwatch.Stop();

                    Console.WriteLine("걸리는 시간 : " + stopwatch.ElapsedMilliseconds + "ms");

                    if (string.IsNullOrEmpty(exceptionMessage))
                    {
                        Console.WriteLine(webResponse.ToString());
                    }
                    else
                    {
                        Console.WriteLine(exceptionMessage);
                    }

                }
                //</API CALL>*/

            }

        }

        private static Series<int, double> get_bigger(Series<int, double> x, Series<int, double> y)
        {
            var return_builder = new SeriesBuilder<int, double>();

            foreach (KeyValuePair<int, Double> value in x.Observations)
            {
                if (value.Value >= y[value.Key])
                {
                    return_builder.Add(value.Key, value.Value);
                }
                else
                {
                    return_builder.Add(value.Key, y[value.Key]);
                }
            }
            var return_series = return_builder.Series;
            return return_series;

        }


    }
}