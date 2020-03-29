import * as React from 'react';

export interface IStepProps {
  position?: number;
}

export default class Step extends React.Component<IStepProps> {
  public render() {
    return <div>{this.props.children}</div>;
  }
}
