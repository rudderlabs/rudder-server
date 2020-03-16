import { ReactComponent as Plus } from '@svg/plus.svg';
import * as React from 'react';
import { Link, RouteComponentProps } from 'react-router-dom';

import { ButtonText, Content, EmptyStyledCard } from './styles';

export interface EmptySourceCardProps extends RouteComponentProps<any> {
  destinationId?: string;
}

export default class EmptySourceCard extends React.Component<any> {
  public render() {
    const { destinationId } = this.props;
    let path = '/sources/setup';
    if (destinationId) {
      path = `/sources/setup/${destinationId}`;
    }
    return (
      <Link to={path}>
        <EmptyStyledCard>
          <Content>
            <Plus />
            <ButtonText> ADD SOURCE</ButtonText>
          </Content>
        </EmptyStyledCard>
      </Link>
    );
  }
}
