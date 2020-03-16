import { LabelDiv } from '@components/common/typography';
import * as React from 'react';
import styled from 'styled-components';

import { Input } from './input';
import { Textarea } from './textarea';

const Container = styled.div`
  width: 475px;
  input {
    width: 100%;
  }
`;

export interface ITextInputFieldProps {
  field: any;
  type: string;
  onChange: (label: string, value: string) => void;
}

export default class TextInputField extends React.Component<
  ITextInputFieldProps
> {
  componentDidMount() {
    const { field, onChange } = this.props;
    onChange(field.value, field.default || '');
  }

  public render() {
    const { field, type, onChange } = this.props;
    return (
      <Container className="p-t-sm">
        <LabelDiv className="m-b-xs">
          {field.label}
          {field.required && ' *'}
        </LabelDiv>
        {field.labelNote && (
          <div className="p-b-sm p-t-xs">{field.labelNote}</div>
        )}
        {type == 'input' ? (
          <Input
            defaultValue={field.default}
            width={500}
            placeholder={field.placeholder}
            onChange={(e: any) => onChange(field.value, e.target.value)}
          />
        ) : (
          <Textarea
            defaultValue={field.default}
            placeholder={field.placeholder}
            onChange={(e: any) => onChange(field.value, e.target.value)}
          />
        )}
      </Container>
    );
  }
}
