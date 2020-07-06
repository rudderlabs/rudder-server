import * as React from 'react';
import { Flex } from './misc';
import styled from 'styled-components';
import { Input } from './input';
import Svg from '@svg/index';
import { Label } from './typography';
import { LabelDiv } from '@components/common/typography';
import { Select } from 'antd';

const { Option } = Select;

const Row = styled(Flex)`
  width: 100%;
  justify-content: 'flex-start';
`;

const Column = styled.div`
  flex: 1;
  input {
    width: 100%;
  }
`;
const ButtonText = styled(Label)`
  margin-left: 10px;
  color: ${props => props.color || props.theme.color.primary300};
`;

const SingleSelectDropdown = styled(Select)<any>`
  &.ant-select {
    width: 100% !important;
  }
  .ant-select-selector {
    background: ${({ theme }) => theme.color.grey50} !important;
    height: 48px !important;
    padding-top: 9px !important;
    padding-bottom: 9px !important;
    border-radius: 2px !important;
  }
`;

export interface ISingleSelectOption {
  name: string;
  value: string;
}

export interface IDynamicSelectFormProps {
  field: any;
  disabled: boolean;
  options: ISingleSelectOption[];
  defaultOption?: ISingleSelectOption;
  onChange: any;
}

export interface IDynamicSelectFormState {
  mapping: any;
  count: any;
}

export default class DynamicSelectForm extends React.Component<
  IDynamicSelectFormProps,
  IDynamicSelectFormState
> {
  constructor(props: IDynamicSelectFormProps) {
    super(props);
    this.state = {
      mapping: [],
      count: 0,
    };
  }

  componentDidMount() {
    const { field, onChange } = this.props;
    if (field.default) {
      this.setState(
        { mapping: [...field.default], count: field.default.length },
        () => onChange(field.value, this.state.mapping),
      );
    } else {
      const keyLeft = field.keyLeft;
      const keyRight = field.keyRight;
      this.setState(
        {
          mapping: [
            {
              [keyLeft]: '',
              [keyRight]: '',
            },
          ],
          count: 1,
        },
        () => onChange(field.value, this.state.mapping),
      );
    }
  }

  public onInputChange = (index: number, value: string, isLeft: boolean) => {
    const { onChange, field } = this.props;
    const keyLeft = field.keyLeft;
    const keyRight = field.keyRight;
    this.setState(
      (prevState: any) => {
        const mapping = prevState.mapping.splice(0);
        const updatedItem = {
          ...mapping[index],
          [isLeft ? keyLeft : keyRight]: value,
        };
        mapping.splice(index, 1, updatedItem);
        return {
          ...prevState,
          mapping,
        };
      },
      () => onChange(field.value, this.state.mapping),
    );
  };

  public renderSingleRow = (index: number) => {
    const { field, options } = this.props;
    const { mapping } = this.state;
    const keyLeft = field.keyLeft;
    const keyRight = field.keyRight;
    return (
      <Row className="m-t-xs m-b-xs" key={index}>
        <Column className="p-r-sm">
          <Input
            value={mapping[index] && mapping[index][keyLeft]}
            placeholder={field.placeholderLeft}
            onChange={(e: any) =>
              this.onInputChange(index, e.target.value, true)
            }
          />
        </Column>
        <Column className="p-l-sm">
          <SingleSelectDropdown
            defaultValue={mapping[index] && mapping[index][keyRight]}
            style={{ width: 120 }}
            onChange={(e: any) => this.onInputChange(index, e, false)}
          >
            {options.map(opt => (
              <Option
                value={opt.value}
                key={`dynamic-select-opt-key-${opt.value}`}
              >
                {opt.name}
              </Option>
            ))}
          </SingleSelectDropdown>
        </Column>
      </Row>
    );
  };

  public render() {
    const { field } = this.props;
    const { count } = this.state;

    return (
      <div>
        <Row>
          <LabelDiv className="m-b-xs">{field.label}</LabelDiv>
        </Row>
        <Row>
          <Column className="p-r-sm">{field.labelLeft}</Column>
          <Column className="p-l-sm">{field.labelRight}</Column>
        </Row>
        {[...Array(count)].map((v: any, index: number) =>
          this.renderSingleRow(index),
        )}
        <a onClick={() => this.setState({ count: this.state.count + 1 })}>
          <div className="p-t-sm">
            <Svg name="plus" />
            <ButtonText> ADD MORE</ButtonText>
          </div>
        </a>
      </div>
    );
  }
}
