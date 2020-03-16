import React, { PureComponent } from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import moment from 'moment';

export default class Stats extends PureComponent<any, any> {
  constructor(props: any) {
    super(props);
    this.state = {
      strokeWidth: {
        all: 1,
        allowed: 1,
        blocked: 1,
      },
    };
  }

  public handleMouseEnter = (o: any) => {
    const { dataKey } = o;
    const { strokeWidth } = this.state;

    this.setState({
      strokeWidth: { ...strokeWidth, [dataKey]: 2 },
    });
  };
  public handleMouseLeave = (o: any) => {
    const { dataKey } = o;
    const { strokeWidth } = this.state;

    this.setState({
      strokeWidth: { ...strokeWidth, [dataKey]: 1 },
    });
  };

  public renderCustomAxisTick = (data: any) => {
    const { range } = this.props;
    const date = new Date(data.payload.value);
    return (
      <g transform={`translate(${data.x},${data.y})`}>
        <text
          x={0}
          y={0}
          dy={16}
          textAnchor="middle"
          fill="#666"
          fontSize="12"
          transform="rotate(-35)"
        >
          {range === 'day'
            ? moment(date).format('HH:mm')
            : moment(date).format('DD/MM')}
        </text>
      </g>
    );
  };

  public render() {
    const { stats, onRefresh, range } = this.props;
    const { strokeWidth } = this.state;
    return (
      <ResponsiveContainer width="100%" maxHeight={500} aspect={4.0 / 3.0}>
        <LineChart
          data={stats}
          margin={{
            top: 25,
            right: 30,
            left: 20,
            bottom: 100,
          }}
        >
          <CartesianGrid strokeDasharray="1 4" />
          <XAxis
            dataKey="timeStamp"
            tick={this.renderCustomAxisTick}
            interval={0}
            ticks={stats.map((s: any) => s.timeStamp)}
            type="number"
            domain={['dataMin', 'dataMax']}
          />
          <YAxis />
          <Tooltip
            itemStyle={{ textTransform: 'capitalize' }}
            labelFormatter={value =>
              range === 'day'
                ? moment(value).format('DD/MM/YYYY HH:mm:ss')
                : moment(value).format('DD/MM/YYYY')
            }
            labelStyle={{ fontWeight: 'bold', marginBottom: '10px' }}
          />
          <Legend
            verticalAlign="top"
            iconType="circle"
            formatter={(value, entry, index) => value.toUpperCase()}
            onMouseEnter={this.handleMouseEnter}
            onMouseLeave={this.handleMouseLeave}
          />
          <Line
            type="monotone"
            dataKey="all"
            connectNulls={true}
            stroke="#4F4F4F"
            activeDot={{ r: 8 }}
            strokeWidth={strokeWidth.all}
          />
          <Line
            type="monotone"
            dataKey="allowed"
            stroke="#71ADFE"
            connectNulls={true}
            activeDot={{ r: 8 }}
            strokeWidth={strokeWidth.allowed}
          />
          <Line
            type="monotone"
            connectNulls={true}
            dataKey="blocked"
            stroke="#EB5757"
            activeDot={{ r: 8 }}
            strokeWidth={strokeWidth.blocked}
          />
        </LineChart>
      </ResponsiveContainer>
    );
  }
}
