import * as React from 'react';
import styled from 'styled-components';
import { TimePicker as AntTimePicker, Button } from 'antd';
import { LabelDiv } from '@components/common/typography';
import moment, { Moment } from 'moment';

const Container = styled.div`
  width: 475px;
`;

const Picker = styled(AntTimePicker)<any>`
  &.ant-picker {
    background: ${({ theme }) => theme.color.grey50};
    width: 100% !important;
  }
`;

export interface ITimePickerOptions {
  omitLabel?: boolean;
  omitSeconds?: boolean;
}

export interface ITimePickerProps {
  field: any;
  options?: ITimePickerOptions;
  onChange: (label: string, value: string) => void;
}

export default class TimePicker extends React.Component<ITimePickerProps, any> {
  constructor(props: any) {
    super(props);
    this.state = { time: props.field.time };
  }

  componentDidMount() {
    const { field } = this.props;
    this.setState({ time: field.default });
  }

  change = (time: Moment | null, timeString: string) => {
    console.log(time, timeString);
    const { field, onChange } = this.props;
    this.setState({ time: time });
    onChange(field.value, timeString);
  };

  public render() {
    const { field, options } = this.props;
    return (
      <Container className="p-t-sm">
        <LabelDiv className="m-b-xs">
          {field.label}
          {field.required && ' *'}
        </LabelDiv>
        <Picker
          onChange={this.change}
          value={this.state.time ? moment(this.state.time, 'HH:mm') : undefined}
          minuteStep={15}
          format={'HH:mm'}
          size="large"
          inputReadOnly={true}
        />
      </Container>
    );
  }
}
