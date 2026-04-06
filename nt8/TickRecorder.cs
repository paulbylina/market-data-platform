#region Using declarations
using System;
using System.IO;
using NinjaTrader.Cbi;
using NinjaTrader.Data;
using NinjaTrader.NinjaScript;
#endregion

// This namespace holds Indicators in this folder and is required. Do not change it.
namespace NinjaTrader.NinjaScript.Indicators
{
	public class TickRecorder : Indicator
	{
		private string outputPath;

		protected override void OnStateChange()
		{
			if (State == State.SetDefaults)
			{
				Description					= "Records L1, L2, and trade events to a local CSV file.";
				Name						= "TickRecorder";
				Calculate					= Calculate.OnEachTick;
				IsOverlay					= true;
				DisplayInDataBox			= false;
				DrawOnPricePanel			= true;
				DrawHorizontalGridLines		= true;
				DrawVerticalGridLines		= true;
				PaintPriceMarkers			= false;
				IsSuspendedWhileInactive	= false;
			}
			else if (State == State.DataLoaded)
			{
				string folder = Path.Combine(
					Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments),
					"NinjaTrader 8",
					"tick_recorder_logs"
				);

				Directory.CreateDirectory(folder);

				outputPath = Path.Combine(
					folder,
					$"{Instrument.FullName}_{DateTime.Now:yyyyMMdd_HHmmss}.csv"
				);

				WriteLine("ts,event_type,instrument,field_1,field_2,field_3,field_4,field_5");
			}
		}

		protected override void OnBarUpdate()
		{
		}
		
		
		// L1 Recorder
		protected override void OnMarketData(MarketDataEventArgs e)
		{
			if (string.IsNullOrEmpty(outputPath))
				return;
		
			string ts = DateTime.UtcNow.ToString("O");
			string instrumentName = Instrument.FullName;
		
			if (e.MarketDataType == MarketDataType.Bid || e.MarketDataType == MarketDataType.Ask)
			{
				string side = e.MarketDataType.ToString();
				string price = e.Price.ToString();
				string size = e.Volume.ToString();
		
				WriteLine($"{ts},QUOTE,{instrumentName},{side},{price},{size},,");
			}
			else if (e.MarketDataType == MarketDataType.Last)
			{
				string price = e.Price.ToString();
				string size = e.Volume.ToString();
		
				WriteLine($"{ts},TRADE,{instrumentName},LAST,{price},{size},,");
			}
		}

		
		// L2 Recorder
		protected override void OnMarketDepth(MarketDepthEventArgs e)
		{
			if (string.IsNullOrEmpty(outputPath))
				return;
		
			string ts = DateTime.UtcNow.ToString("O");
			string eventType = "MARKET_DEPTH";
			string instrumentName = Instrument.FullName;
			string operation = e.Operation.ToString();
			string side = e.MarketDataType.ToString();
			string level = e.Position.ToString();
			string price = e.Price.ToString();
			string size = e.Volume.ToString();
		
			WriteLine($"{ts},{eventType},{instrumentName},{operation},{side},{level},{price},{size}");
		}

		private void WriteLine(string line)
		{
			File.AppendAllText(outputPath, line + Environment.NewLine);
		}
	}
}

#region NinjaScript generated code. Neither change nor remove.

namespace NinjaTrader.NinjaScript.Indicators
{
	public partial class Indicator : NinjaTrader.Gui.NinjaScript.IndicatorRenderBase
	{
		private TickRecorder[] cacheTickRecorder;
		public TickRecorder TickRecorder()
		{
			return TickRecorder(Input);
		}

		public TickRecorder TickRecorder(ISeries<double> input)
		{
			if (cacheTickRecorder != null)
				for (int idx = 0; idx < cacheTickRecorder.Length; idx++)
					if (cacheTickRecorder[idx] != null &&  cacheTickRecorder[idx].EqualsInput(input))
						return cacheTickRecorder[idx];
			return CacheIndicator<TickRecorder>(new TickRecorder(), input, ref cacheTickRecorder);
		}
	}
}

namespace NinjaTrader.NinjaScript.MarketAnalyzerColumns
{
	public partial class MarketAnalyzerColumn : MarketAnalyzerColumnBase
	{
		public Indicators.TickRecorder TickRecorder()
		{
			return indicator.TickRecorder(Input);
		}

		public Indicators.TickRecorder TickRecorder(ISeries<double> input )
		{
			return indicator.TickRecorder(input);
		}
	}
}

namespace NinjaTrader.NinjaScript.Strategies
{
	public partial class Strategy : NinjaTrader.Gui.NinjaScript.StrategyRenderBase
	{
		public Indicators.TickRecorder TickRecorder()
		{
			return indicator.TickRecorder(Input);
		}

		public Indicators.TickRecorder TickRecorder(ISeries<double> input )
		{
			return indicator.TickRecorder(input);
		}
	}
}

#endregion
