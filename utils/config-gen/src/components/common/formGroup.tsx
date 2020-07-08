import * as React from 'react';
import { withTheme } from 'styled-components';
import _ from 'lodash';

import TextInputField from './textInput';
import { SubHeaderDiv } from './typography';
import SwitchInput from './switchInput';
import SingleSelect from './singleSelect';
import TimePicker from './timePicker';
import DynamicForm from './dynamicForm';
import { toJS } from 'mobx';
import DynamicCustomForm from './dynamicCustomForm';
import DynamicSelectForm from './dynamicSelectForm';

export interface IFormGroupProps {
  title: string;
  fields: any;
  theme: any;
  onStateChange: any;
  disabled: boolean;
  initialSettings?: any;
}

export interface IFormGroupState {
  formData: any;
  error: boolean;
  blurCount: number;
  errorMessage: string;
}

class FormGroup extends React.Component<IFormGroupProps, IFormGroupState> {
  constructor(props: IFormGroupProps) {
    super(props);
    this.state = {
      formData: {},
      error: false,
      blurCount: 0,
      errorMessage: 'Wrong format',
    };
  }
  public onChange = (label: string, value: string) => {
    const { onStateChange } = this.props;
    this.setState(
      (prevState: any) => ({
        formData: {
          ...prevState.formData,
          [label]: value,
        },
      }),
      () => onStateChange(this.state.formData),
    );
  };

  public onBlur = (regexJSON: any) => {
    const { formData, blurCount } = this.state;
    let key = formData[Object.keys(formData)[0]];
    var regex = RegExp(regexJSON);
    if (blurCount === 0) {
      this.setState({ blurCount: blurCount + 1 });
    } else {
      this.setState({ error: !regex.test(key), blurCount: blurCount + 1 });
    }
  };

  public renderField = (field: any) => {
    const { initialSettings, title, disabled } = this.props;
    if (initialSettings && initialSettings[field.value] !== undefined) {
      field.default = toJS(initialSettings[field.value]);
    }
    switch (field.type) {
      case 'textInput':
      case 'textareaInput':
        return (
          <div className="p-b-sm">
            <div
              className="width-100"
              onBlur={() => {
                this.onBlur(field.regex);
              }}
            >
              <TextInputField
                field={field}
                onChange={this.onChange}
                type={field.type === 'textInput' ? 'input' : 'textarea'}
                disabled={disabled}
              ></TextInputField>
              {/* {this.state.error ? (
                <ErrorLabel
                  error={this.state.error}
                  errorMessage={this.state.errorMessage}
                />
              ) : null} */}
              {field.footerNote && (
                <div className="p-t-sm p-b-sm">{field.footerNote}</div>
              )}
              {field.footerURL && (
                <div className="p-t-sm p-b-sm">
                  <a href={field.footerURL.link}>{field.footerURL.text}</a>
                </div>
              )}
            </div>
          </div>
        );
      case 'singleSelect':
        return (
          <div className="p-b-sm">
            <SingleSelect
              field={field}
              options={field.options}
              defaultOption={field.defaultOption}
              onChange={this.onChange}
            ></SingleSelect>
          </div>
        );
      case 'timePicker':
        return (
          <div className="p-b-sm">
            <TimePicker
              field={field}
              options={field.options}
              onChange={this.onChange}
            ></TimePicker>
            {field.footerNote && (
              <div className="p-t-sm p-b-sm">{field.footerNote}</div>
            )}
          </div>
        );
      case 'checkbox':
        return (
          <div className="p-b-sm">
            <SwitchInput
              field={field}
              onChange={this.onChange}
              hidden={false}
              disabled={disabled}
            ></SwitchInput>
            {field.footerNote && (
              <div className="p-t-sm p-b-sm">{field.footerNote}</div>
            )}
          </div>
        );
      case 'dynamicForm':
        return (
          <div className="p-b-sm">
            <DynamicForm
              field={field}
              onChange={this.onChange}
              disabled={disabled}
              hidden={field.hidden}
            ></DynamicForm>
            {field.footerNote && (
              <div className="p-t-sm p-b-sm">{field.footerNote}</div>
            )}
          </div>
        );
      case 'dynamicCustomForm':
        return (
          <div className="p-b-sm">
            <DynamicCustomForm
              field={field}
              onChange={this.onChange}
              disabled={disabled}
            ></DynamicCustomForm>
            {field.footerNote && (
              <div className="p-t-sm p-b-sm">{field.footerNote}</div>
            )}
          </div>
        );
      case 'dynamicSelectForm':
        return (
          <div className="p-b-sm">
            <DynamicSelectForm
              field={field}
              onChange={this.onChange}
              options={field.options}
              disabled={disabled}
            ></DynamicSelectForm>
            {field.footerNote && (
              <div className="p-t-sm p-b-sm">{field.footerNote}</div>
            )}
          </div>
        );
      case 'defaultCheckbox':
        return (
          <div className="p-b-sm">
            This is a <strong>device-mode only</strong> destination. Please add
            Factory in your implementation.
            <SwitchInput
              field={field}
              hidden={false}
              onChange={this.onChange}
              disabled={true}
            ></SwitchInput>
            {field.footerNote && (
              <div className="p-t-sm p-b-sm">{field.footerNote}</div>
            )}
          </div>
        );

      default:
        break;
    }
  };
  public render() {
    const { title, fields, theme } = this.props;
    return (
      <div className="p-b-md p-t-sm">
        <SubHeaderDiv color={theme.color.black} className="p-b-sm">
          {title}
        </SubHeaderDiv>
        {fields.map((field: any) => this.renderField(field))}
      </div>
    );
  }
}

export default withTheme(FormGroup);
