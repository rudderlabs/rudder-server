import * as React from 'react';
import { ButtonSecondary } from '@components/common/button';
import { StyledSteps, DotsContainer } from './styles';
import { StyledStepsFooter } from './styles';
import { Dot } from '@components/common/dot';

export interface IFormStep {}

export interface IStepsProps {
  current: number;
  onCancel?: (event: React.MouseEvent<HTMLElement>) => any;
  onNext?: (event: React.MouseEvent<HTMLElement>) => any;
  enableNext?: boolean;
  showNextLoader: boolean;
}

export default class Steps extends React.Component<IStepsProps> {
  public static defaultProps = {
    current: 0,
    showNextLoader: false,
  };
  public render() {
    let stepCount = 0;
    let dotStepCount = 0;
    return (
      <StyledSteps>
        {React.Children.map(this.props.children, (child: any) => {
          const childProps = {
            ...child.props,
            position: child.props.position || stepCount,
          };
          stepCount++;
          return (
            childProps.position === this.props.current &&
            React.cloneElement(child, childProps)
          );
        })}
        <StyledStepsFooter spaceBetween alignItems="center">
          <ButtonSecondary onClick={this.props.onCancel}>
            Cancel
          </ButtonSecondary>
          <DotsContainer>
            {React.Children.map(this.props.children, (child: any) => {
              const childProps = {
                ...child.props,
                position: child.props.position || dotStepCount,
              };
              dotStepCount++;
              return (
                <Dot active={childProps.position === this.props.current} />
              );
            })}
          </DotsContainer>
          <ButtonSecondary
            onClick={this.props.onNext}
            disabled={!this.props.enableNext}
            loading={this.props.showNextLoader}
          >
            Next
          </ButtonSecondary>
        </StyledStepsFooter>
      </StyledSteps>
    );
  }
}
