#region Using declarations
using System;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.IO;
using NinjaTrader.Cbi;
using NinjaTrader.Data;
using NinjaTrader.Gui.Tools;
using NinjaTrader.NinjaScript;
#endregion

namespace NinjaTrader.NinjaScript.Indicators
{
    public class Level2Recorder : Indicator
    {
        private StreamWriter writer;
        private string filePath;
        private long rowCount;
        private readonly object fileLock = new object();

        [NinjaScriptProperty]
        [Display(Name = "Output Folder", Order = 1, GroupName = "Recorder")]
        public string OutputFolder { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "Flush Every Rows", Order = 2, GroupName = "Recorder")]
        public int FlushEveryRows { get; set; }

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Description = "Records raw Level 2 market depth updates to CSV.";
                Name = "Level2Recorder";

                Calculate = Calculate.OnEachTick;
                IsOverlay = true;
                DisplayInDataBox = false;
                DrawOnPricePanel = false;
                PaintPriceMarkers = false;
                IsSuspendedWhileInactive = false;

                OutputFolder = @"C:\REPOSITORY\trading-dev-framework\ninja-lake\raw\ninjatrader\L2";
                FlushEveryRows = 1000;
            }
            else if (State == State.DataLoaded)
            {
                StartWriter();
            }
            else if (State == State.Terminated)
            {
                StopWriter();
            }
        }

        protected override void OnBarUpdate()
        {
            // No chart calculation needed.
            // This indicator only records OnMarketDepth events.
        }

        protected override void OnMarketDepth(MarketDepthEventArgs marketDepthUpdate)
        {
            if (writer == null || marketDepthUpdate == null)
                return;

            string eventTime = marketDepthUpdate.Time.ToString("O", CultureInfo.InvariantCulture);

            string line = string.Format(
                CultureInfo.InvariantCulture,
                "{0},{1},{2},{3},{4},{5},{6},{7},{8}",
                Csv(eventTime),
                Csv(marketDepthUpdate.Instrument != null ? marketDepthUpdate.Instrument.FullName : Instrument.FullName),
                Csv(marketDepthUpdate.MarketDataType.ToString()),
                Csv(marketDepthUpdate.Operation.ToString()),
                marketDepthUpdate.Position,
                marketDepthUpdate.Price,
                marketDepthUpdate.Volume,
                Csv(marketDepthUpdate.MarketMaker),
                marketDepthUpdate.IsReset
            );

            lock (fileLock)
            {
                writer.WriteLine(line);
                rowCount++;

                if (FlushEveryRows > 0 && rowCount % FlushEveryRows == 0)
                    writer.Flush();
            }
        }

        private void StartWriter()
        {
            Directory.CreateDirectory(OutputFolder);

            string instrumentName = SafeFileName(Instrument.FullName);
            string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);

            filePath = Path.Combine(OutputFolder, instrumentName + "_L2_" + timestamp + ".csv");

            writer = new StreamWriter(filePath, true);
            writer.WriteLine("event_time,instrument,side,operation,position,price,volume,market_maker,is_reset");
            writer.Flush();

            Print("Level2Recorder writing to: " + filePath);
        }

        private void StopWriter()
        {
            lock (fileLock)
            {
                if (writer != null)
                {
                    writer.Flush();
                    writer.Close();
                    writer.Dispose();
                    writer = null;
                }
            }
        }

        private string Csv(string value)
        {
            if (value == null)
                value = "";

            return "\"" + value.Replace("\"", "\"\"") + "\"";
        }

        private string SafeFileName(string value)
        {
            foreach (char c in Path.GetInvalidFileNameChars())
                value = value.Replace(c, '_');

            return value.Replace(" ", "_");
        }
    }
}