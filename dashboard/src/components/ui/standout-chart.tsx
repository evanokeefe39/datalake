import { useEffect, useState } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { Card, CardHeader, CardTitle } from "@/components/ui/card";

interface DayDatum {
  day: number;
  standout_count: number;
}

export function StandoutChart() {
  const [data, setData] = useState<DayDatum[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/weekly-summary")
      .then((r) => r.json())
      .then(setData)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  const total = data.reduce((s, d) => s + d.standout_count, 0);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Standout Posts by Day</CardTitle>
        <span className="text-[10px] text-muted font-data">
          {total} posts &gt;1&sigma; above creator mean
        </span>
      </CardHeader>
      {loading ? (
        <div className="h-[240px] flex items-center justify-center text-muted font-data text-xs animate-pulse">
          LOADING
        </div>
      ) : (
        <div className="h-[240px] -ml-4">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={data} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
              <CartesianGrid
                strokeDasharray="3 3"
                stroke="#100e0e"
                vertical={false}
              />
              <XAxis
                dataKey="day"
                tick={{ fill: "#8a8080", fontSize: 11, fontFamily: "JetBrains Mono" }}
                axisLine={{ stroke: "#100e0e" }}
                tickLine={false}
                label={{
                  value: "Day of Month",
                  position: "insideBottom",
                  offset: -5,
                  fill: "#8a8080",
                  fontSize: 10,
                  fontFamily: "JetBrains Mono",
                }}
              />
              <YAxis
                tick={{ fill: "#8a8080", fontSize: 11, fontFamily: "JetBrains Mono" }}
                axisLine={false}
                tickLine={false}
                allowDecimals={false}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: "#2c2727",
                  border: "1px solid #100e0e",
                  borderRadius: 0,
                  fontFamily: "JetBrains Mono",
                  fontSize: 11,
                  color: "#C9D4D4",
                }}
                formatter={(value: number) => [value, "Standout Posts"]}
                labelFormatter={(day: number) => `Day ${day}`}
              />
              <Bar
                dataKey="standout_count"
                fill="#E2BDB1"
                radius={[0, 0, 0, 0]}
                maxBarSize={24}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </Card>
  );
}
