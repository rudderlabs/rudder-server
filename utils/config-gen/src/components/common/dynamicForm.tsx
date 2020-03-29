import * as React from 'react';
import { Flex } from './misc';
import styled from 'styled-components';
import { Input } from './input';
import Svg from '@svg/index';
import { Label } from './typography';

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

export interface IDynamicFormProps {
  field: any;
  onChange: any;
}
export interface IDynamicFormState {
  mapping: any;
  count: any;
}

export default class DynamicForm extends React.Component<
  IDynamicFormProps,
  IDynamicFormState
> {
  constructor(props: IDynamicFormProps) {
    super(props);
    const keyLeft = props.field.keyLeft;
    const keyRight = props.field.keyRight;
    this.state = {
      mapping: [
        {
          [keyLeft]: '',
          [keyRight]: '',
        },
      ],
      count: 1,
    };
  }

  componentDidMount() {
    const { field, onChange } = this.props;
    if (field.default) {
      this.setState(
        { mapping: field.default, count: field.default.length },
        () => onChange(field.value, this.state.mapping),
      );
    }
  }

  public onConfigChange = (index: number, value: string, isLeft: boolean) => {
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
    const { field } = this.props;
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
              this.onConfigChange(index, e.target.value, true)
            }
          />
        </Column>
        <Column className="p-l-sm">
          <Input
            value={mapping[index] && mapping[index][keyRight]}
            placeholder={field.placeholderRight}
            onChange={(e: any) =>
              this.onConfigChange(index, e.target.value, false)
            }
          />
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
