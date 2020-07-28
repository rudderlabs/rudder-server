import * as React from 'react';
import { Flex } from './misc';
import styled from 'styled-components';
import { Input } from './input';
import Svg from '@svg/index';
import { Label } from './typography';
import { LabelDiv } from '@components/common/typography';
import { SubHeaderDiv } from './typography';
import TextInputField from './textInput';
import SwitchInput from './switchInput';
import DynamicForm from './dynamicForm';

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

export interface IDynamicCustomFormProps {
  field: any;
  onChange: any;
  disabled: boolean;
}
export interface IDynamicCustomFormState {
  mapping: any;
  count: any;
}

export default class DynamicCustomForm extends React.Component<
  IDynamicCustomFormProps,
  IDynamicCustomFormState
> {
  constructor(props: IDynamicCustomFormProps) {
    super(props);
    const fields = props.field.customFields;
    let fieldData = new Map();
    fields.map((field: any) => {
      let key: string = field.value;
      fieldData.set(
        key,
        field.default ? field.default : this.getDefaultValue(field),
      );
    });
    const obj = Object.fromEntries(fieldData);
    this.state = {
      mapping: [{ ...obj }],
      count: 1,
    };
  }

  componentDidMount() {
    const { field, onChange } = this.props;
    if (field.default) {
      this.setState(
        { mapping: [...field.default], count: field.default.length },
        () => onChange(field.value, this.state.mapping),
      );
    }
  }

  public onConfigChange = (index: number, value: string, keyValue: string) => {
    const { onChange, field } = this.props;
    this.setState(
      (prevState: any) => {
        const mapping = prevState.mapping.splice(0);
        const updatedItem = {
          ...mapping[index],
          [keyValue]: value,
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
    return (
      <div className="p-b-md p-t-sm">
        {field.customFields.map((customField: any) =>
          this.renderField({ ...customField }, index),
        )}
      </div>
    );
  };

  public render() {
    const { field } = this.props;
    const { count } = this.state;
    return (
      <div className="p-b-md p-t-sm">
        <div>
          {[...Array(count)].map((v: any, index: number) =>
            this.renderSingleRow(index),
          )}
          <a
            onClick={() => {
              this.setState({ count: this.state.count + 1 });
            }}
          >
            <div className="p-t-sm">
              <Svg name="plus" />
              <ButtonText> ADD MORE</ButtonText>
            </div>
          </a>
        </div>
      </div>
    );
  }

  public renderField = (customField: any, index: any) => {
    const { field, disabled } = this.props;

    if (field.default && field.default[index]) {
      let defaultValue = field.default[index][customField.value];
      customField.default = defaultValue;
    } else {
      customField.default = this.getDefaultValue(customField);
    }

    switch (customField.type) {
      case 'textInput':
      case 'textareaInput':
        return (
          <div className="p-b-sm">
            <div>
              <TextInputField
                field={customField}
                onChange={(label, value) => {
                  this.onConfigChange(index, value, label);
                }}
                type={customField.type == 'textInput' ? 'input' : 'textarea'}
                disabled={disabled}
              ></TextInputField>
            </div>
            {customField.footerNote && (
              <div className="p-t-sm p-b-sm">{customField.footerNote}</div>
            )}
          </div>
        );
      case 'checkbox':
        return (
          <div className="p-b-sm">
            <SwitchInput
              field={customField}
              hidden={false}
              onChange={(label, value) => {
                this.onConfigChange(index, value, label);
              }}
              disabled={disabled}
            ></SwitchInput>
            {customField.footerNote && (
              <div className="p-t-sm p-b-sm">{customField.footerNote}</div>
            )}
          </div>
        );
      case 'dynamicForm':
        return (
          <div className="p-b-sm">
            <DynamicForm
              field={customField}
              onChange={(label: any, value: any) => {
                this.onConfigChange(index, value, label);
              }}
              disabled={disabled}
            ></DynamicForm>
            {customField.footerNote && (
              <div className="p-t-sm p-b-sm">{customField.footerNote}</div>
            )}
          </div>
        );

      default:
        break;
    }
  };

  getDefaultValue(field: any) {
    let fieldType = field.type;

    switch (fieldType) {
      case 'textInput':
      case 'textareaInput':
        return '';
      case 'checkbox':
        return false;
      case 'dynamicForm':
        const keyLeft = field.keyLeft;
        const keyRight = field.keyRight;
        return {
          [keyLeft]: '',
          [keyRight]: '',
        };
    }
  }
}
